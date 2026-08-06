import asyncio
import json
import uuid
from typing import (
    AsyncIterator,
    Protocol,
    Callable,
)

from fastapi import FastAPI, APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel


# ============================================================
# DOMAIN MODELS
# ============================================================

class ChatRequest(BaseModel):
    session_id: str
    message: str



# ============================================================
# PORTS
# ============================================================

class ChatEventsPort(Protocol):
    """
    Application output port.
    The application emits chat events here.
    """

    async def token(
        self,
        value: str,
    ) -> None:
        ...


    async def final(
        self,
        value: str,
    ) -> None:
        ...


    async def error(
        self,
        value: str,
    ) -> None:
        ...


    async def complete(
        self,
    ) -> None:
        ...



class AgentPort(Protocol):

    async def run(
        self,
        state: AgentState,
        config: dict,
    ) -> AgentState:
        ...



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


        try:

            result = await self.agent.run(
                state,
                config,
            )


            await self.events.final(
                result.final_answer
            )


        except Exception as exc:

            await self.events.error(
                str(exc)
            )


        finally:

            await self.events.complete()



# ============================================================
# OUTBOUND ADAPTER
# Queue implementation of ChatEventsPort
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



    async def complete(
        self,
    ):

        await self.queue.put(
            {
                "type": "complete"
            }
        )



# ============================================================
# LANGGRAPH OUTBOUND ADAPTER
# ============================================================

class LangGraphAgentAdapter(AgentPort):


    def __init__(
        self,
        graph,
    ):

        self.graph = graph



    async def run(
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
# LANGGRAPH CALLBACK ADAPTER
# ============================================================

class TokenCallbackAdapter:


    def __init__(
        self,
        events: ChatEventsPort,
    ):

        self.events = events



    async def __call__(
        self,
        token: str,
    ):

        await self.events.token(
            token
        )



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
                session_id,
                message,
            )
        )


        async def event_stream():


            while True:


                event = await events.queue.get()


                if event["type"] == "complete":

                    yield (
                        "event: done\n"
                        "data: {}\n\n"
                    )

                    break



                yield (
                    f"event: {event['type']}\n"
                    f"data: {json.dumps(event)}\n\n"
                )



        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
        )



# ============================================================
# ROUTER
# ============================================================

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



# Your lifespan:

async def lifespan(app: FastAPI):

    container = Container()

    ...


    #
    # Create event adapter
    #

    events = SSEQueueAdapter()


    #
    # Create callback adapter
    #

    callback = TokenCallbackAdapter(
        events
    )


    #
    # Keep your build_agent contract
    #

    graph, _ = build_agent(
        llm=llm,
        tool_registry=tool_registry,
        checkpointer=checkpointer,
        use_streaming=True,
        on_token=callback,
    )


    app.state.graph = graph


    yield



app = FastAPI(
    lifespan=lifespan
)


app.include_router(
    create_chat_router(
        app.state.graph
    )
)