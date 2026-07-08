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
    async def _execute(self, request: httpx.Request):
        return await self._breaker.call_async(
            self._transport.handle_async_request,
            request,
        )

    async def handle_async_request(
        self,
        request: httpx.Request,
    ) -> httpx.Response:
        return await self._execute(request)

    async def aclose(self):
        await self._transport.aclose()