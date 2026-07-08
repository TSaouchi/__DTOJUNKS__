from datetime import timedelta

import httpx
from aiobreaker import CircuitBreaker
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


class ResilientTransport(httpx.AsyncBaseTransport):
    def __init__(self):
        self._transport = httpx.AsyncHTTPTransport()
        self._breaker = CircuitBreaker(
            fail_max=5,
            timeout_duration=timedelta(seconds=30),
        )

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
    )
    async def _retry_execute(self, request: httpx.Request) -> httpx.Response:
        # No breaker logic here. This just absorbs transient failures.
        # The breaker only sees the outcome after retries are exhausted.
        return await self._transport.handle_async_request(request)

    async def handle_async_request(
        self,
        request: httpx.Request,
    ) -> httpx.Response:
        # Breaker wraps retry: if open, fails fast (no wasted retries).
        # If closed, the retry loop runs to completion and exactly one
        # success/failure is recorded against the breaker's fail_max.
        return await self._breaker.call_async(self._retry_execute, request)

    async def aclose(self):
        await self._transport.aclose()


client = httpx.AsyncClient(
    transport=ResilientTransport(),
    timeout=30,
)

from langchain_openai import ChatOpenAI

llm = ChatOpenAI(
    model="gpt-4.1",
    api_key="...",
    http_async_client=client,
)