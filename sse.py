import asyncio
import contextvars
import json
from typing import Optional

from fastapi import APIRouter, FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------
# ContextVar used by the LLM callback
# ---------------------------------------------------------------------

_current_queue: contextvars.ContextVar[Optional[asyncio.Queue]] = (
    contextvars.ContextVar("current_queue", default=None)
)


async def on_token(token: str):
    queue = _current_queue.get()
    if queue:
        await queue.put(token)


# ---------------------------------------------------------------------
# Request Model
# ---------------------------------------------------------------------

class ChatRequest(BaseModel):
    session_id: str
    message: str


# ---------------------------------------------------------------------
# Use Case
# ---------------------------------------------------------------------

class StreamChatUseCase:

    def __init__(self, graph):
        self.graph = graph

    async def execute(self, session_id: str, message: str):

        config = {
            "configurable": {
                "thread_id": session_id,
            }
        }

        snapshot = await self.graph.aget_state(config)

        if snapshot.values:
            state = _unpack(snapshot.values)
        else:
            state = AgentState(session_id=session_id)

        state.iteration = 0
        state.conversation.append(
            ConversationMessage(
                role=Role.USER,
                content=message,
            )
        )

        queue: asyncio.Queue = asyncio.Queue()

        async def run_agent():

            token = _current_queue.set(queue)

            try:
                result = await self.graph.ainvoke(
                    _pack(state),
                    config=config,
                )

                final_state = _unpack(result)

                await queue.put(
                    {
                        "type": "final",
                        "answer": final_state.final_answer,
                    }
                )

            except Exception as e:
                await queue.put(
                    {
                        "type": "error",
                        "message": str(e),
                    }
                )

            finally:
                _current_queue.reset(token)
                await queue.put(None)

        asyncio.create_task(run_agent())

        while True:

            item = await queue.get()

            if item is None:
                yield "event: done\ndata: {}\n\n"
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


# ---------------------------------------------------------------------
# Controller
# ---------------------------------------------------------------------

class ChatController:

    def __init__(self, graph):
        self.use_case = StreamChatUseCase(graph)

    async def stream(self, request: ChatRequest):

        return StreamingResponse(
            self.use_case.execute(
                session_id=request.session_id,
                message=request.message,
            ),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )


# ---------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------

router = APIRouter()


def create_chat_routes(graph):

    controller = ChatController(graph)

    @router.post("/chat/stream")
    async def chat_stream(request: ChatRequest):
        return await controller.stream(request)

    return router


# ---------------------------------------------------------------------
# App
# ---------------------------------------------------------------------

app = FastAPI()

# After creating your graph during lifespan:
#
# app.include_router(create_chat_routes(app.state.graph))