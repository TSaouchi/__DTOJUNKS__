import logging
from datetime import timedelta

import httpx
from aiobreaker import CircuitBreaker, CircuitBreakerListener
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


class BreakerLogger(CircuitBreakerListener):
    def __init__(self, logger: logging.Logger):
        self._logger = logger
        self._last_exc = None

    def failure(self, cb, exc):
        self._last_exc = exc
        self._logger.warning(
            "breaker failure %s/%s recorded: %r", cb.fail_counter, cb.fail_max, exc
        )

    def success(self, cb):
        self._last_exc = None

    def state_change(self, cb, old_state, new_state):
        self._logger.info("breaker %s -> %s", old_state.state, new_state.state)
        if type(new_state).__name__ == "CircuitOpenState":
            self._logger.error(
                "breaker OPEN — %s consecutive failures reached fail_max=%s. "
                "Last error: %r. Will allow a trial call again after %s.",
                cb.fail_counter, cb.fail_max, self._last_exc, cb.timeout_duration,
            )


class ResilientTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        logger: logging.Logger,
        transport: httpx.AsyncBaseTransport | None = None,
        fail_max: int = 5,
        timeout_duration: timedelta = timedelta(seconds=30),
    ):
        self._logger = logger
        self._transport = transport or httpx.AsyncHTTPTransport()

        self._breaker_logger = BreakerLogger(logger)
        self._breaker = CircuitBreaker(
            fail_max=fail_max,
            timeout_duration=timeout_duration,
            listeners=[self._breaker_logger],
        )

        # Built once, reused for every call — no decorator needed.
        self._retryer = AsyncRetrying(
            retry=retry_if_exception_type(httpx.HTTPError),
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            reraise=True,
            before_sleep=self._log_retry,  # bound method, self is already captured
        )

    def _log_retry(self, retry_state) -> None:
        exc = retry_state.outcome.exception()
        self._logger.warning(
            "retry attempt %s failed (%r); retrying in %.1fs",
            retry_state.attempt_number,
            exc,
            retry_state.next_action.sleep,
        )

    async def _retry_execute(self, request: httpx.Request) -> httpx.Response:
        return await self._retryer(self._transport.handle_async_request, request)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return await self._breaker.call_async(self._retry_execute, request)

    async def aclose(self):
        await self._transport.aclose()