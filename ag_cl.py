"""
===========================================================
FULL LLM AGENT ARCHITECTURE (LangGraph + Reflection Loop)
===========================================================

DESIGN PRINCIPLES
─────────────────
• Hexagonal architecture
  - langchain/langgraph/sqlglot imports confined inside the method or
    __init__ where they are used.
  - Domain types (Conversation, ConversationMessage, AgentState,
    ToolCapability, Node) are pure Python — zero framework imports.
  - Conversation.to_langchain() is the single boundary crossing point
    from domain messages to LangChain messages.

• Full async — every node, tool, and LLM call is async.

• Strict SRP per node:
    PlannerNode    — calls LLM, appends ASSISTANT message to Conversation
    RouterNode     — reads last_node + state fields only; owns ALL routing
    ExecutorNode   — reads tool_calls from last ASSISTANT message,
                     dispatches concurrently, appends TOOL messages
    MemoryNode     — trims Conversation to token budget (offline)
    ReflectionNode — calls LLM, stores ReflectionDecision; never mutates Conversation
    FeedbackNode   — appends USER critique message on retry
    FinalNode      — writes final_answer; never mutates Conversation

• Single owner per concern:
    - ASSISTANT messages  → PlannerNode only
    - TOOL messages       → ExecutorNode only
    - USER messages       → caller (initial) + FeedbackNode (retry critique)
    - Routing decisions   → RouterNode only

• Typed state — AgentState is a Pydantic BaseModel with typed fields.
  LangGraph receives a TypedDict wrapper that serialises AgentState as JSON.

NODE GRAPH
──────────
  START → planner → [router] → executor → memory → [router] (tool loop)
                    [router] → reflection → [router] → feedback → [router]
                                           [router] → final → END
===========================================================
"""

from __future__ import annotations

import asyncio
import re as _re
import tempfile
import os
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import TypedDict


# ════════════════════════════════════════════════════════════
# DOMAIN — CONVERSATION
# Pure Python. Zero framework imports.
# ════════════════════════════════════════════════════════════

class Role(str, Enum):
    USER      = "user"
    ASSISTANT = "assistant"
    TOOL      = "tool"
    SYSTEM    = "system"


@dataclass
class ConversationMessage:
    """
    A single turn in the conversation.
    tool_calls is populated on ASSISTANT messages when the LLM requests tools.
    tool_call_id is populated on TOOL messages to pair them with the request.
    """
    role: Role
    content: str
    tool_calls: list[ToolCall] = field(default_factory=list)
    tool_call_id: Optional[str] = None


class Conversation:
    """
    Owns the ordered message history.
    Provides the single boundary-crossing method to_langchain() that
    converts domain messages into LangChain objects — confined here so
    no other class needs to import LangChain message types.
    """

    def __init__(self, messages: list[ConversationMessage] | None = None) -> None:
        self.messages: list[ConversationMessage] = messages or []

    def append(self, msg: ConversationMessage) -> None:
        self.messages.append(msg)

    def last(self) -> Optional[ConversationMessage]:
        return self.messages[-1] if self.messages else None

    def last_assistant(self) -> Optional[ConversationMessage]:
        for msg in reversed(self.messages):
            if msg.role == Role.ASSISTANT:
                return msg
        return None

    def copy(self) -> "Conversation":
        return Conversation(list(self.messages))

    def to_langchain(self) -> list[Any]:
        """
        Convert domain messages to LangChain BaseMessage objects.
        This is the ONLY place in the codebase where LangChain message
        classes are instantiated from domain data.
        """
        # LangChain imports confined to this method
        from langchain_core.messages import (
            AIMessage,
            HumanMessage,
            SystemMessage,
            ToolMessage,
        )

        out: list[Any] = []
        for m in self.messages:
            if m.role == Role.USER:
                out.append(HumanMessage(content=m.content))

            elif m.role == Role.ASSISTANT:
                lc_tool_calls = [
                    {"id": tc.id, "name": tc.name, "args": tc.args}
                    for tc in m.tool_calls
                ]
                out.append(AIMessage(content=m.content, tool_calls=lc_tool_calls))

            elif m.role == Role.TOOL:
                out.append(
                    ToolMessage(
                        content=m.content,
                        tool_call_id=m.tool_call_id or "",
                    )
                )
            else:
                out.append(SystemMessage(content=m.content))
        return out

    @classmethod
    def from_langchain(cls, lc_messages: list[Any]) -> "Conversation":
        """
        Reconstruct a Conversation from LangChain messages after trimming.
        Used by MemoryNode to convert the trimmed list back to domain type.
        """
        from langchain_core.messages import (
            AIMessage,
            HumanMessage,
            SystemMessage,
            ToolMessage,
        )

        msgs: list[ConversationMessage] = []
        for m in lc_messages:
            if isinstance(m, HumanMessage):
                msgs.append(ConversationMessage(role=Role.USER, content=m.content or ""))
            elif isinstance(m, AIMessage):
                tcs = [
                    ToolCall(id=tc["id"], name=tc["name"], args=tc.get("args", {}))
                    for tc in (m.tool_calls or [])
                ]
                msgs.append(ConversationMessage(
                    role=Role.ASSISTANT,
                    content=m.content or "",
                    tool_calls=tcs,
                ))
            elif isinstance(m, ToolMessage):
                msgs.append(ConversationMessage(
                    role=Role.TOOL,
                    content=m.content or "",
                    tool_call_id=m.tool_call_id,
                ))
            elif isinstance(m, SystemMessage):
                msgs.append(ConversationMessage(role=Role.SYSTEM, content=m.content or ""))
        return cls(msgs)


# ════════════════════════════════════════════════════════════
# DOMAIN — TOOL MODELS
# ════════════════════════════════════════════════════════════

class ToolCall(BaseModel):
    """A tool invocation requested by the planner."""
    id: str
    name: str
    args: dict[str, Any] = Field(default_factory=dict)


class ToolResult(BaseModel):
    """The outcome of a single tool execution."""
    id: str
    output: str
    error: Optional[str] = None

    @property
    def content(self) -> str:
        return self.output if self.error is None else f"Error: {self.error}"


# ════════════════════════════════════════════════════════════
# DOMAIN — LLM OUTPUT SCHEMAS
# ════════════════════════════════════════════════════════════

class PlannerDecision(BaseModel):
    """
    Structured output from PlannerNode's LLM call.
    tool_calls XOR answer — the LLM must not populate both.
    notes contains chain-of-thought reasoning (not shown to user).
    """
    tool_calls: list[ToolCall] = Field(default_factory=list)
    answer: Optional[str] = None
    notes: Optional[str] = None

    @property
    def wants_tools(self) -> bool:
        return len(self.tool_calls) > 0


class ReflectionAction(str, Enum):
    ACCEPT = "accept"
    RETRY  = "retry"


class ReflectionDecision(BaseModel):
    """Structured output from ReflectionNode's LLM call."""
    action: ReflectionAction
    critique: str

    @property
    def should_retry(self) -> bool:
        return self.action == ReflectionAction.RETRY


# ════════════════════════════════════════════════════════════
# DOMAIN — AGENT STATE
# Pydantic BaseModel for typed field access everywhere.
# ════════════════════════════════════════════════════════════

class AgentState(BaseModel):
    """
    Single source of truth for the agent session.

    conversation    : full message history as domain objects
    planner         : last PlannerDecision, set by PlannerNode
    reflection      : last ReflectionDecision, set by ReflectionNode
    last_node       : tag written by every node on exit; RouterNode's only input
    session_id      : unique per run
    iteration       : incremented by PlannerNode on every LLM call
    max_iterations  : hard cap checked by RouterNode
    final_answer    : written by FinalNode
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    conversation:   Conversation           = Field(default_factory=Conversation)
    planner:        Optional[PlannerDecision]   = None
    reflection:     Optional[ReflectionDecision] = None
    last_node:      str                    = ""
    session_id:     str                    = ""
    iteration:      int                    = 0
    max_iterations: int                    = 6
    final_answer:   Optional[str]          = None


# ════════════════════════════════════════════════════════════
# LANGGRAPH STATE WRAPPER
# LangGraph requires TypedDict. We serialise AgentState to/from JSON.
# ════════════════════════════════════════════════════════════

class GraphState(TypedDict):
    """LangGraph-compatible wrapper. Stores AgentState as a JSON dict."""
    state: dict  # AgentState.model_dump() result


def _pack(state: AgentState) -> GraphState:
    """Serialise AgentState → GraphState for LangGraph."""
    def ser_conv(conv: Conversation) -> list[dict]:
        return [
            {
                "role": m.role.value,
                "content": m.content,
                "tool_calls": [tc.model_dump() for tc in m.tool_calls],
                "tool_call_id": m.tool_call_id,
            }
            for m in conv.messages
        ]

    return {
        "state": {
            "conversation":   ser_conv(state.conversation),
            "planner":        state.planner.model_dump() if state.planner else None,
            "reflection":     state.reflection.model_dump() if state.reflection else None,
            "last_node":      state.last_node,
            "session_id":     state.session_id,
            "iteration":      state.iteration,
            "max_iterations": state.max_iterations,
            "final_answer":   state.final_answer,
        }
    }


def _unpack(gs: GraphState) -> AgentState:
    """Deserialise GraphState → AgentState."""
    d = gs["state"]

    conv = Conversation([
        ConversationMessage(
            role=Role(m["role"]),
            content=m["content"],
            tool_calls=[ToolCall(**tc) for tc in (m.get("tool_calls") or [])],
            tool_call_id=m.get("tool_call_id"),
        )
        for m in (d.get("conversation") or [])
    ])

    return AgentState(
        conversation=conv,
        planner=PlannerDecision(**d["planner"]) if d.get("planner") else None,
        reflection=ReflectionDecision(**d["reflection"]) if d.get("reflection") else None,
        last_node=d.get("last_node", ""),
        session_id=d.get("session_id", ""),
        iteration=d.get("iteration", 0),
        max_iterations=d.get("max_iterations", 6),
        final_answer=d.get("final_answer"),
    )


# ════════════════════════════════════════════════════════════
# TOOL INPUT SCHEMAS
# ════════════════════════════════════════════════════════════

class SQLToolInput(BaseModel):
    query:   str = Field(...,        description="SQL query to execute.")
    dialect: str = Field("oracle",   description="sqlglot source dialect.")


class PythonToolInput(BaseModel):
    code: str = Field(..., description="Python source code to run in a sandbox.")


# ════════════════════════════════════════════════════════════
# TOOL PORT  (pure Python ABC — zero framework imports)
# ════════════════════════════════════════════════════════════

class ToolCapability(ABC):
    """Domain contract for any tool. Subclasses must not import AI frameworks."""

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def description(self) -> str: ...

    @property
    @abstractmethod
    def args_schema(self) -> type[BaseModel]: ...

    @abstractmethod
    async def execute(self, **kwargs: Any) -> ToolResult: ...


# ════════════════════════════════════════════════════════════
# SQL TOOL
# ════════════════════════════════════════════════════════════

class SQLToolCapability(ToolCapability):

    def __init__(self, database: Any, default_dialect: str = "oracle") -> None:
        self._db = database
        self._default_dialect = default_dialect

    @property
    def name(self) -> str:
        return "sql_executor"

    @property
    def description(self) -> str:
        return "Validate and execute SQL. Defaults to Oracle dialect."

    @property
    def args_schema(self) -> type[BaseModel]:
        return SQLToolInput

    async def execute(self, **kwargs: Any) -> ToolResult:
        import sqlglot
        import sqlglot.errors

        call_id: str = kwargs.pop("_call_id", "")
        query:   str = kwargs.get("query", "")
        dialect: str = kwargs.get("dialect", self._default_dialect)

        if not query.strip():
            return ToolResult(id=call_id, output="", error="No SQL query provided.")

        try:
            statements = sqlglot.parse(
                query, dialect=dialect, error_level=sqlglot.ErrorLevel.RAISE
            )
        except sqlglot.errors.SqlglotError as exc:
            return ToolResult(id=call_id, output="", error=f"SQL validation error ({dialect}): {exc}")

        if not statements:
            return ToolResult(id=call_id, output="", error="Empty or unparseable query.")

        transpiled = ";\n".join(
            stmt.sql(dialect="sqlite") for stmt in statements if stmt is not None
        )

        try:
            result = await self._db.execute(transpiled)
            return ToolResult(id=call_id, output=str(result))
        except Exception as exc:
            return ToolResult(id=call_id, output="", error=f"SQL execution error: {exc}")


# ════════════════════════════════════════════════════════════
# PYTHON TOOL
# ════════════════════════════════════════════════════════════

class PythonToolCapability(ToolCapability):
    """Executes Python via a temp file in a subprocess — never exec()."""

    _TIMEOUT:    int = 10
    _MAX_OUTPUT: int = 4_000

    @property
    def name(self) -> str:
        return "python_executor"

    @property
    def description(self) -> str:
        return "Execute Python code in a sandboxed subprocess. Returns stdout."

    @property
    def args_schema(self) -> type[BaseModel]:
        return PythonToolInput

    async def execute(self, **kwargs: Any) -> ToolResult:
        call_id: str = kwargs.pop("_call_id", "")
        code:    str = kwargs.get("code", "").strip()

        if not code:
            return ToolResult(id=call_id, output="", error="No code provided.")

        tmp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".py", delete=False, encoding="utf-8"
            ) as tmp:
                tmp.write(code)
                tmp_path = tmp.name

            try:
                proc = await asyncio.create_subprocess_exec(
                    "python3", tmp_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=self._TIMEOUT
                )
            except asyncio.TimeoutError:
                return ToolResult(
                    id=call_id, output="",
                    error=f"Execution timed out after {self._TIMEOUT}s."
                )
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

        out = stdout.decode("utf-8", errors="replace").strip()
        err = stderr.decode("utf-8", errors="replace").strip()

        if proc.returncode != 0:
            return ToolResult(id=call_id, output="", error=err or "Non-zero exit code.")

        output = (out + (f"\nSTDERR:\n{err}" if err else "") or "(no output)")[: self._MAX_OUTPUT]
        return ToolResult(id=call_id, output=output)


# ════════════════════════════════════════════════════════════
# TOOL REGISTRY
# ════════════════════════════════════════════════════════════

class ToolRegistry:

    def __init__(self, tools: list[ToolCapability]) -> None:
        self._tools: dict[str, ToolCapability] = {t.name: t for t in tools}

    def get(self, name: str) -> ToolCapability:
        tool = self._tools.get(name)
        if tool is None:
            raise KeyError(f"No tool registered: '{name}'.")
        return tool

    def descriptions(self) -> str:
        return "\n".join(
            f"  • {t.name} — {t.description}" for t in self._tools.values()
        )


# ════════════════════════════════════════════════════════════
# NODE BASE CLASS
# ════════════════════════════════════════════════════════════

class Node(ABC):
    """
    Every node receives the unpacked AgentState, mutates a copy,
    and returns the updated AgentState packed as GraphState.
    Every implementation MUST set state.last_node before returning.
    """

    @abstractmethod
    async def __call__(self, gs: GraphState) -> GraphState: ...

    @staticmethod
    def _unpack(gs: GraphState) -> AgentState:
        return _unpack(gs)

    @staticmethod
    def _pack(state: AgentState) -> GraphState:
        return _pack(state)


# ════════════════════════════════════════════════════════════
# PLANNER NODE
# Responsibility: call LLM, append ASSISTANT message to Conversation.
# ════════════════════════════════════════════════════════════

class PlannerNode(Node):
    """
    Invokes the LLM with structured output and immediately appends
    the resulting ASSISTANT ConversationMessage to state.conversation.

    Stable UUID tool call IDs are generated here so ExecutorNode can
    pair ToolResults back to requests without re-querying state.
    """

    _SYSTEM = (
        "You are an expert assistant.\n"
        "Available tools:\n"
        "{tools}\n\n"
        "Rules:\n"
        "  - Always fill `notes` with your reasoning first.\n"
        "  - Populate `tool_calls` to use tools, OR `answer` to reply directly.\n"
        "  - Never populate both tool_calls and answer."
    )

    def __init__(self, llm: Any, registry: ToolRegistry) -> None:
        from langchain_core.messages import SystemMessage
        self._llm = llm.with_structured_output(PlannerDecision)
        self._system = SystemMessage(
            content=self._SYSTEM.format(tools=registry.descriptions())
        )

    async def __call__(self, gs: GraphState) -> GraphState:
        state = self._unpack(gs)

        lc_messages = [self._system, *state.conversation.to_langchain()]
        decision: PlannerDecision = await self._llm.ainvoke(lc_messages)

        # Assign stable UUIDs to tool calls
        tool_calls = [
            ToolCall(
                id=f"call_{uuid.uuid4().hex[:8]}",
                name=tc.name,
                args=tc.args,
            )
            for tc in decision.tool_calls
        ]
        decision = PlannerDecision(
            tool_calls=tool_calls,
            answer=decision.answer,
            notes=decision.notes,
        )

        state.conversation.append(ConversationMessage(
            role=Role.ASSISTANT,
            content=decision.answer or decision.notes or "",
            tool_calls=tool_calls,
        ))
        state.planner   = decision
        state.iteration += 1
        state.last_node  = "planner"

        return self._pack(state)


# ════════════════════════════════════════════════════════════
# STREAMING PLANNER NODE
# Responsibility: identical contract to PlannerNode, streams tokens.
# ════════════════════════════════════════════════════════════

class StreamingPlannerNode(Node):

    _SYSTEM = PlannerNode._SYSTEM

    def __init__(self, llm: Any, registry: ToolRegistry) -> None:
        from langchain_core.messages import SystemMessage
        self._llm    = llm
        self._system = SystemMessage(
            content=self._SYSTEM.format(tools=registry.descriptions())
        )

    async def __call__(self, gs: GraphState) -> GraphState:
        from langchain_core.messages import AIMessage

        state = self._unpack(gs)
        lc_messages = [self._system, *state.conversation.to_langchain()]

        chunks: list[Any] = []
        async for chunk in self._llm.astream(lc_messages):
            chunks.append(chunk)
            if hasattr(chunk, "content") and chunk.content:
                print(chunk.content, end="", flush=True)
        print()

        last: AIMessage = chunks[-1] if chunks else AIMessage(content="")
        raw_tcs = getattr(last, "tool_calls", None) or []

        tool_calls = [
            ToolCall(
                id=f"call_{uuid.uuid4().hex[:8]}",
                name=tc["name"],
                args=tc.get("args", {}),
            )
            for tc in raw_tcs
        ]
        decision = PlannerDecision(
            tool_calls=tool_calls,
            answer=last.content if not raw_tcs else None,
            notes=last.content if raw_tcs else None,
        )

        state.conversation.append(ConversationMessage(
            role=Role.ASSISTANT,
            content=last.content or "",
            tool_calls=tool_calls,
        ))
        state.planner   = decision
        state.iteration += 1
        state.last_node  = "planner"

        return self._pack(state)


# ════════════════════════════════════════════════════════════
# ROUTER NODE
# Responsibility: read last_node + typed state fields → return node name.
# Zero Conversation access. Zero LLM calls. Zero state mutation.
# ════════════════════════════════════════════════════════════

class RouterNode(Node):
    """
    Routing table
    ─────────────
    Iteration cap checked first — always → "final" when exhausted.

    last_node == "planner"
        planner.wants_tools → "executor"
        else                → "reflection"

    last_node == "memory"   → "planner"   (continue tool loop)
    last_node == "feedback" → "planner"   (re-plan with critique)

    last_node == "reflection"
        reflection.should_retry → "feedback"
        else                    → "final"
    """

    async def __call__(self, gs: GraphState) -> str:  # type: ignore[override]
        state = self._unpack(gs)

        if state.iteration >= state.max_iterations:
            return "final"

        match state.last_node:
            case "planner":
                return "executor" if (state.planner and state.planner.wants_tools) else "reflection"
            case "memory" | "feedback":
                return "planner"
            case "reflection":
                return "feedback" if (state.reflection and state.reflection.should_retry) else "final"
            case _:
                return "final"


# ════════════════════════════════════════════════════════════
# EXECUTOR NODE
# Responsibility: dispatch tool_calls from last ASSISTANT message,
#                 append TOOL ConversationMessages.
# ════════════════════════════════════════════════════════════

class ExecutorNode(Node):
    """
    Reads tool_calls from the last ASSISTANT ConversationMessage
    (set by PlannerNode), executes all tools concurrently, and
    appends TOOL ConversationMessages for each result.

    Tools receive _call_id as a private kwarg so ToolResult can
    carry the matching id without it leaking into the tool schema.
    """

    def __init__(self, registry: ToolRegistry) -> None:
        self._registry = registry

    async def __call__(self, gs: GraphState) -> GraphState:
        state = self._unpack(gs)

        last_assistant = state.conversation.last_assistant()
        if not last_assistant or not last_assistant.tool_calls:
            state.last_node = "executor"
            return self._pack(state)

        async def _run(tc: ToolCall) -> ToolResult:
            try:
                tool = self._registry.get(tc.name)
                return await tool.execute(_call_id=tc.id, **tc.args)
            except KeyError:
                return ToolResult(id=tc.id, output="", error=f"Unknown tool: '{tc.name}'.")
            except Exception as exc:  # noqa: BLE001
                return ToolResult(id=tc.id, output="", error=str(exc))

        results: tuple[ToolResult, ...] = await asyncio.gather(
            *[_run(tc) for tc in last_assistant.tool_calls]
        )

        for result in results:
            state.conversation.append(ConversationMessage(
                role=Role.TOOL,
                content=result.content,
                tool_call_id=result.id,
            ))

        state.last_node = "executor"
        return self._pack(state)


# ════════════════════════════════════════════════════════════
# MEMORY NODE
# Responsibility: trim Conversation to stay within the token budget.
# ════════════════════════════════════════════════════════════

def _count_tokens(text: str) -> int:
    """
    Approximate BPE token count — stdlib re only, no external deps.
    Each word = 1 token; each punctuation run within a word = +1 token.
    Within ~10% of cl100k_base on prose, code, and SQL.
    """
    if not text:
        return 0
    return sum(
        1 + len(_re.findall(r"[^a-zA-Z0-9]+", word))
        for word in text.split()
    )


def count_message_tokens(messages: list) -> int:
    """Callable for trim_messages(token_counter=...). 4-token overhead per message."""
    return sum(
        _count_tokens(
            msg.content if isinstance(msg.content, str) else str(msg.content)
        ) + 4
        for msg in messages
    )


class MemoryNode(Node):
    """
    Converts Conversation → LangChain messages, trims, converts back.
    Fully offline — no LLM, no network, no external dependencies.
    """

    def __init__(self, max_tokens: int = 8_000) -> None:
        self._max_tokens = max_tokens

    async def __call__(self, gs: GraphState) -> GraphState:
        from langchain_core.messages import trim_messages

        state = self._unpack(gs)
        lc_messages = state.conversation.to_langchain()

        trimmed = trim_messages(
            lc_messages,
            token_counter=count_message_tokens,
            max_tokens=self._max_tokens,
            strategy="last",
            include_system=True,
            start_on="human",
        )

        state.conversation = Conversation.from_langchain(trimmed)
        state.last_node    = "memory"
        return self._pack(state)


# ════════════════════════════════════════════════════════════
# REFLECTION NODE
# Responsibility: evaluate last ASSISTANT message via LLM.
#                 Never mutates Conversation.
# ════════════════════════════════════════════════════════════

class ReflectionNode(Node):
    """
    One-shot LLM call to evaluate the last ASSISTANT message.
    Reads from conversation.last_assistant() — guaranteed to exist
    because PlannerNode always appends an ASSISTANT message.
    Never mutates state.conversation.
    """

    _SYSTEM = (
        "You are a self-correction evaluator. "
        "Assess the assistant's last response for correctness and completeness. "
        "Return action='accept' if satisfactory, 'retry' if improvement is needed."
    )

    def __init__(self, llm: Any) -> None:
        from langchain_core.messages import SystemMessage
        self._llm    = llm.with_structured_output(ReflectionDecision)
        self._system = SystemMessage(content=self._SYSTEM)

    async def __call__(self, gs: GraphState) -> GraphState:
        from langchain_core.messages import HumanMessage

        state = self._unpack(gs)

        last = state.conversation.last_assistant()
        answer = last.content if last else "(no assistant answer found)"

        decision: ReflectionDecision = await self._llm.ainvoke([
            self._system,
            HumanMessage(content=f"Evaluate this answer:\n\n{answer}"),
        ])

        state.reflection = decision
        state.last_node  = "reflection"
        return self._pack(state)


# ════════════════════════════════════════════════════════════
# FEEDBACK NODE
# Responsibility: append USER critique message when reflection says retry.
# ════════════════════════════════════════════════════════════

class FeedbackNode(Node):
    """
    Translates ReflectionDecision.critique into a USER ConversationMessage
    so PlannerNode sees exactly what was wrong on its next invocation.

    A USER message is used so trim_messages(start_on="human") always
    preserves it and never trims it away mid-session.
    """

    async def __call__(self, gs: GraphState) -> GraphState:
        state = self._unpack(gs)

        critique = (
            state.reflection.critique
            if state.reflection
            else "No specific critique provided."
        )

        state.conversation.append(ConversationMessage(
            role=Role.USER,
            content=(
                f"Your previous answer was not satisfactory.\n"
                f"Critique: {critique}\n\n"
                f"Please try again, addressing the critique above."
            ),
        ))
        state.last_node = "feedback"
        return self._pack(state)


# ════════════════════════════════════════════════════════════
# FINAL NODE
# Responsibility: write final_answer from last ASSISTANT message.
#                 Never mutates Conversation.
# ════════════════════════════════════════════════════════════

class FinalNode(Node):
    """
    Reads the last ASSISTANT ConversationMessage and writes its content
    to state.final_answer. No LLM calls. No Conversation mutation.
    """

    async def __call__(self, gs: GraphState) -> GraphState:
        state = self._unpack(gs)

        last = state.conversation.last_assistant()
        state.final_answer = last.content if last else "No answer was produced."
        state.last_node    = "final"
        return self._pack(state)


# ════════════════════════════════════════════════════════════
# GRAPH BUILDER
# ════════════════════════════════════════════════════════════

class AgentGraph:
    """
    Compiles all nodes into a LangGraph StateGraph.
    RouterNode owns ALL conditional routing — no lambdas with routing logic.
    """

    def __init__(
        self,
        planner:    PlannerNode | StreamingPlannerNode,
        router:     RouterNode,
        executor:   ExecutorNode,
        memory:     MemoryNode,
        reflection: ReflectionNode,
        feedback:   FeedbackNode,
        final:      FinalNode,
    ) -> None:
        self._planner    = planner
        self._router     = router
        self._executor   = executor
        self._memory     = memory
        self._reflection = reflection
        self._feedback   = feedback
        self._final      = final

    def build(self) -> Any:
        from langgraph.graph import END, StateGraph

        g = StateGraph(GraphState)

        g.add_node("planner",    self._planner)
        g.add_node("executor",   self._executor)
        g.add_node("memory",     self._memory)
        g.add_node("reflection", self._reflection)
        g.add_node("feedback",   self._feedback)
        g.add_node("final",      self._final)

        g.set_entry_point("planner")

        _targets = {
            "planner":    "planner",
            "executor":   "executor",
            "reflection": "reflection",
            "feedback":   "feedback",
            "final":      "final",
        }

        # Single RouterNode instance handles all conditional branching
        g.add_conditional_edges("planner",    self._router, _targets)
        g.add_conditional_edges("memory",     self._router, _targets)
        g.add_conditional_edges("reflection", self._router, _targets)
        g.add_conditional_edges("feedback",   self._router, _targets)

        g.add_edge("executor", "memory")
        g.add_edge("final",    END)

        return g.compile()


# ════════════════════════════════════════════════════════════
# FACTORY
# ════════════════════════════════════════════════════════════

def build_agent(
    llm: Any,
    database: Any,
    sql_dialect:    str  = "oracle",
    max_tokens:     int  = 8_000,
    max_iterations: int  = 6,
    use_streaming:  bool = False,
) -> tuple[Any, GraphState]:
    """
    Wire all dependencies and return (compiled_graph, initial_graph_state).

    Parameters
    ----------
    llm            : any async LangChain chat model
    database       : object with async execute(sql: str) -> Any
    sql_dialect    : sqlglot source dialect — default "oracle"
    max_tokens     : offline memory trim budget in approximate tokens
    max_iterations : hard cap on planner→executor cycles
    use_streaming  : stream tokens to stdout during planning
    """
    registry = ToolRegistry([
        SQLToolCapability(database, default_dialect=sql_dialect),
        PythonToolCapability(),
    ])

    planner: PlannerNode | StreamingPlannerNode = (
        StreamingPlannerNode(llm, registry)
        if use_streaming
        else PlannerNode(llm, registry)
    )

    graph = AgentGraph(
        planner=planner,
        router=RouterNode(),
        executor=ExecutorNode(registry),
        memory=MemoryNode(max_tokens=max_tokens),
        reflection=ReflectionNode(llm),
        feedback=FeedbackNode(),
        final=FinalNode(),
    ).build()

    initial_state = AgentState(max_iterations=max_iterations)
    return graph, _pack(initial_state)


# ════════════════════════════════════════════════════════════
# EXAMPLE / SMOKE TEST
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":

    class MockLLM:
        def __init__(self, schema: type[BaseModel] | None = None) -> None:
            self._schema = schema

        def with_structured_output(self, schema: type[BaseModel]) -> "MockLLM":
            return MockLLM(schema=schema)

        async def ainvoke(self, messages: list, **_: Any) -> Any:
            if self._schema is PlannerDecision:
                return PlannerDecision(
                    notes="Simple factual question — answer directly.",
                    tool_calls=[],
                    answer="The answer is 42.",
                )
            if self._schema is ReflectionDecision:
                return ReflectionDecision(
                    action=ReflectionAction.ACCEPT,
                    critique="Answer is correct and complete.",
                )
            from langchain_core.messages import AIMessage
            return AIMessage(content="mock")

        async def astream(self, messages: list, **_: Any):
            from langchain_core.messages import AIMessage
            for token in ["The ", "answer ", "is 42."]:
                yield AIMessage(content=token)

    class MockDB:
        async def execute(self, sql: str) -> str:
            return f"[mock result for: {sql[:60]}]"

    async def main() -> None:
        llm = MockLLM()
        db  = MockDB()

        graph, initial_gs = build_agent(
            llm=llm,
            database=db,
            sql_dialect="oracle",
            max_tokens=8_000,
            max_iterations=6,
        )

        # Start with a user message
        start_state = _unpack(initial_gs)
        start_state.session_id = str(uuid.uuid4())
        start_state.conversation.append(ConversationMessage(
            role=Role.USER,
            content="What is the meaning of life?",
        ))

        result_gs: GraphState = await graph.ainvoke(_pack(start_state))
        result = _unpack(result_gs)

        # Verify invariants
        last = result.conversation.last_assistant()
        assert last is not None, "Last message must be an ASSISTANT message"
        assert result.final_answer == last.content, (
            "final_answer must equal last ASSISTANT message content"
        )
        assert result.reflection is not None, "ReflectionNode must have run"
        assert isinstance(result.reflection, ReflectionDecision), (
            "reflection must be a ReflectionDecision instance"
        )
        assert result.reflection.action == ReflectionAction.ACCEPT

        print("=== AGENT RESULT ===")
        print(f"Answer     : {result.final_answer}")
        print(f"Iterations : {result.iteration}")
        print(f"Reflection : {result.reflection.action.value} — {result.reflection.critique}")
        print(f"Last node  : {result.last_node}")
        print(f"Messages   : {len(result.conversation.messages)}")
        print("All assertions passed ✓")

    asyncio.run(main())
