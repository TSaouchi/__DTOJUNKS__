import asyncio
import contextvars
import json
import uuid
from typing import (
    AsyncIterator,
    Optional,
    Protocol,
)

from fastapi import FastAPI, APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel


# ============================================================
# PORTS
# ============================================================

class ChatEventsPort(Protocol):

    async def on_token(self, token: str) -> None:
        ...

    async def on_final_answer(self, answer: str) -> None:
        ...

    async def on_error(self, message: str) -> None:
        ...

    async def on_complete(self) -> None:
        ...


class AgentPort(Protocol):

    async def execute(
        self,
        state: AgentState,
        config: dict,
    ) -> AgentState:
        ...


# ============================================================
# CALLBACK BRIDGE
# Used by build_agent(on_token=on_token)
# ============================================================

_current_events: contextvars.ContextVar[
    Optional[ChatEventsPort]
] = contextvars.ContextVar(
    "current_events",
    default=None,
)


async def on_token(token: str):

    events = _current_events.get()

    if events:
        await events.on_token(token)


# ============================================================
# QUEUE ADAPTER
# Infrastructure implementation of ChatEventsPort
# ============================================================

class QueueChatEventsAdapter(ChatEventsPort):

    def __init__(self):

        self.queue: asyncio.Queue = asyncio.Queue()


    async def on_token(
        self,
        token: str,
    ):

        await self.queue.put(token)


    async def on_final_answer(
        self,
        answer: str,
    ):

        await self.queue.put(
            {
                "type": "final",
                "answer": answer,
            }
        )


    async def on_error(
        self,
        message: str,
    ):

        await self.queue.put(
            {
                "type": "error",
                "message": message,
            }
        )


    async def on_complete(self):

        await self.queue.put(None)



# ============================================================
# LANGGRAPH ADAPTER
# ============================================================

class LangGraphAgentAdapter(AgentPort):

    def __init__(
        self,
        graph,
    ):

        self.graph = graph


    async def execute(
        self,
        state: AgentState,
        config: dict,
    ) -> AgentState:


        result = await self.graph.ainvoke(
            _pack(state),
            config=config,
        )

        return _unpack(result)



# ============================================================
# USE CASE
# ============================================================

class StreamChatUseCase:


    def __init__(
        self,
        agent: AgentPort,
        events: ChatEventsPort,
    ):

        self.agent = agent
        self.events = events



    async def execute(
        self,
        session_id: str,
        message: str,
    ):

        state = AgentState(
            session_id=session_id,
        )

        state.iteration = 0


        state.conversation.append(
            ConversationMessage(
                role=Role.USER,
                content=message,
            )
        )


        config = {
            "configurable": {
                "thread_id": session_id
            }
        }


        # Bind current request events
        context_token = _current_events.set(
            self.events
        )


        try:

            final_state = await self.agent.execute(
                state,
                config,
            )


            await self.events.on_final_answer(
                final_state.final_answer
            )


        except Exception as e:

            await self.events.on_error(
                str(e)
            )


        finally:

            _current_events.reset(
                context_token
            )

            await self.events.on_complete()



# ============================================================
# CONTROLLER
# ============================================================

class ChatController:


    def __init__(
        self,
        graph,
    ):

        self.agent = LangGraphAgentAdapter(
            graph
        )


    async def stream(
        self,
        session_id: str,
        message: str,
    ):


        events = QueueChatEventsAdapter()


        use_case = StreamChatUseCase(
            agent=self.agent,
            events=events,
        )


        asyncio.create_task(
            use_case.execute(
                session_id=session_id,
                message=message,
            )
        )


        async def generator() -> AsyncIterator[str]:


            while True:

                item = await events.queue.get()


                if item is None:

                    yield (
                        "event: done\n"
                        "data: {}\n\n"
                    )

                    break



                if isinstance(item, str):

                    yield (
                        "event: token\n"
                        f"data: {json.dumps({'token': item})}\n\n"
                    )


                elif item["type"] == "final":

                    yield (
                        "event: final\n"
                        f"data: {json.dumps(item)}\n\n"
                    )


                elif item["type"] == "error":

                    yield (
                        "event: error\n"
                        f"data: {json.dumps(item)}\n\n"
                    )



        return StreamingResponse(
            generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )



# ============================================================
# ROUTER
# ============================================================

class ChatRequest(BaseModel):

    session_id: str
    message: str



def create_chat_router(
    graph,
):

    router = APIRouter()

    controller = ChatController(
        graph
    )


    @router.post("/chat/stream")
    async def chat(
        request: ChatRequest,
    ):

        return await controller.stream(
            request.session_id,
            request.message,
        )


    return router



# ============================================================
# APPLICATION COMPOSITION
# ============================================================

app = FastAPI()



# Your existing lifespan stays the same:
#
# graph, _ = build_agent(
#     llm=llm,
#     tool_registry=tool_registry,
#     checkpointer=checkpointer,
#     use_streaming=True,
#     on_token=on_token,
# )
#
# app.state.graph = graph


# after graph creation:
#
# app.include_router(
#     create_chat_router(
#         app.state.graph
#     )
# )