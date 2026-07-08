from datetime import timedelta
from typing import Optional

import httpx
from aiobreaker import CircuitBreaker, CircuitBreakerListener
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


def _log_retry(retry_state):
    exc = retry_state.outcome.exception()
    print(
        f"[retry] attempt {retry_state.attempt_number} failed "
        f"({exc!r}); retrying in {retry_state.next_action.sleep:.1f}s"
    )


class BreakerLogger(CircuitBreakerListener):
    def __init__(self):
        self._last_exc: Optional[BaseException] = None

    def failure(self, cb, exc):
        self._last_exc = exc
        print(f"[breaker] failure {cb.fail_counter}/{cb.fail_max} recorded: {exc!r}")

    def success(self, cb):
        self._last_exc = None  # streak reset

    def state_change(self, cb, old_state, new_state):
        print(f"[breaker] {old_state.state} -> {new_state.state}")
        if type(new_state).__name__ == "CircuitOpenState":
            print(
                f"[breaker] OPEN — {cb.fail_counter} consecutive failures reached "
                f"fail_max={cb.fail_max}. Last error: {self._last_exc!r}. "
                f"Will allow a trial call again after {cb.timeout_duration}."
            )


class ResilientTransport(httpx.AsyncBaseTransport):
    def __init__(self):
        self._transport = httpx.AsyncHTTPTransport()
        self._breaker_logger = BreakerLogger()
        self._breaker = CircuitBreaker(
            fail_max=5,
            timeout_duration=timedelta(seconds=30),
            listeners=[self._breaker_logger],
        )

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
        before_sleep=_log_retry,
    )
    async def _retry_execute(self, request: httpx.Request) -> httpx.Response:
        return await self._transport.handle_async_request(request)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return await self._breaker.call_async(self._retry_execute, request)

    async def aclose(self):
        await self._transport.aclose()