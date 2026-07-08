class ResilientTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        transport: httpx.AsyncBaseTransport | None = None,
        fail_max: int = 5,
        timeout_duration: timedelta = timedelta(seconds=30),
    ):
        self._transport = transport or httpx.AsyncHTTPTransport()
        self._breaker_logger = BreakerLogger()
        self._breaker = CircuitBreaker(
            fail_max=fail_max,
            timeout_duration=timeout_duration,
            listeners=[self._breaker_logger],
        )
    # ... rest unchanged


import asyncio
from datetime import timedelta

import httpx


class FlakyTransport(httpx.AsyncBaseTransport):
    """Fails the first `fail_times` calls, or always if `always_fail=True`."""

    def __init__(self, fail_times: int = 0, always_fail: bool = False):
        self.fail_times = fail_times
        self.always_fail = always_fail
        self.calls = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        if self.always_fail or self.calls <= self.fail_times:
            raise httpx.ConnectTimeout("simulated failure", request=request)
        return httpx.Response(200, request=request)


async def main():
    request = httpx.Request("GET", "https://example.com")

    print("=== 1) Transient failure — retry should recover it ===")
    t = ResilientTransport(transport=FlakyTransport(fail_times=2))
    resp = await t.handle_async_request(request)
    print(f"-> got {resp.status_code}\n")

    print("=== 2) Persistent failure — breaker should trip (fail_max=2 for speed) ===")
    t = ResilientTransport(
        transport=FlakyTransport(always_fail=True),
        fail_max=2,
        timeout_duration=timedelta(seconds=2),
    )
    for i in range(3):
        try:
            await t.handle_async_request(request)
        except Exception as e:
            print(f"call {i + 1} -> {type(e).__name__}: {e}")

    print("\n=== 3) Waiting past timeout_duration, breaker should try a trial call ===")
    await asyncio.sleep(2.5)
    try:
        await t.handle_async_request(request)
    except Exception as e:
        print(f"trial call -> {type(e).__name__}: {e}")

    await t.aclose()


asyncio.run(main())