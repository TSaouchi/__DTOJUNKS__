import asyncio
import contextvars
import json
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

    async def token(self, value: str) -> None:
        ...

    async def final(self, value: str) -> None:
        ...

    async def error(self, value: str) -> None:
        ...

    async def complete(self) -> None:
        ...


class AgentPort(Protocol):

    async def run(
        self,
        state,
        config: dict,
    ):
        ...


# ============================================================
# CALLBACK BRIDGE
#
# This is passed to build_agent()
#
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
        await events.token(token)



# ============================================================
# OUTBOUND ADAPTER
#
# Queue implementation of ChatEventsPort
#
# ============================================================

class SSEQueueAdapter(ChatEventsPort):

    def __init__(self):

        self.queue: asyncio.Queue = asyncio.Queue()


    async def token(
        self,
        value: str,
    ):

        await self.queue.put(
            {
                "type": "token",
                "value": value,
            }
        )


    async def final(
        self,
        value: str,
    ):

        await self.queue.put(
            {
                "type": "final",
                "value": value,
            }
        )


    async def error(
        self,
        value: str,
    ):

        await self.queue.put(
            {
                "type": "error",
                "value": value,
            }
        )


    async def complete(self):

        await self.queue.put(
            {
                "type": "complete"
            }
        )



# ============================================================
# OUTBOUND ADAPTER
#
# LangGraph implementation of AgentPort
#
# ============================================================

class LangGraphAgentAdapter(AgentPort):

    def __init__(
        self,
        graph,
    ):

        self.graph = graph


    async def run(
        self,
        state,
        config: dict,
    ):

        result = await self.graph.ainvoke(
            _pack(state),
            config=config,
        )

        return _unpack(result)



# ============================================================
# APPLICATION USE CASE
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
                "thread_id": session_id,
            }
        }


        # Bind this request's event stream
        context_token = _current_events.set(
            self.events
        )


        try:

            final_state = await self.agent.run(
                state,
                config,
            )


            await self.events.final(
                final_state.final_answer
            )


        except Exception as exc:

            await self.events.error(
                str(exc)
            )


        finally:

            _current_events.reset(
                context_token
            )

            await self.events.complete()



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


        events = SSEQueueAdapter()


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



        async def event_generator()
            -> AsyncIterator[str]:


            while True:

                event = await events.queue.get()


                if event["type"] == "complete":

                    yield (
                        "event: done\n"
                        "data: {}\n\n"
                    )

                    break



                if event["type"] == "token":

                    yield (
                        "event: token\n"
                        f"data: {json.dumps({'token': event['value']})}\n\n"
                    )


                elif event["type"] == "final":

                    yield (
                        "event: final\n"
                        f"data: {json.dumps({'answer': event['value']})}\n\n"
                    )


                elif event["type"] == "error":

                    yield (
                        "event: error\n"
                        f"data: {json.dumps({'message': event['value']})}\n\n"
                    )



        return StreamingResponse(
            event_generator(),
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


    @router.post(
        "/chat/stream"
    )
    async def chat(
        request: ChatRequest,
    ):

        return await controller.stream(
            request.session_id,
            request.message,
        )


    return router



# ============================================================
# COMPOSITION ROOT
# ============================================================

app = FastAPI()



# ============================================================
# YOUR EXISTING LIFESPAN
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    container = Container()

    await container.boot


    # ------------------------------
    # Your existing setup
    # ------------------------------

    llm = ChatOpenAI(
        base_url="http://nautilus:1234/v1",
        api_key="lm_studio",
        model="mistralai/ministral-3-3b",
        http_async_client=client,
    )


    tool_registry = ToolRegistry(
        [
            SQLToolCapability(
                sqlite_repository,
                SQLHandlerFactory(),
            ),
            PythonToolCapability(
                SafeCodeFactory()
            ),
        ]
    )


    checkpointer = MemorySaver()



    # IMPORTANT:
    # keep your build_agent exactly
    #

    graph, _ = build_agent(
        llm=llm,
        tool_registry=tool_registry,
        checkpointer=checkpointer,
        use_streaming=True,
        on_token=on_token,
    )


    app.state.graph = graph


    yield



app = FastAPI(
    lifespan=lifespan
)


# after startup:
#
# app.include_router(
#     create_chat_router(
#         app.state.graph
#     )
# )