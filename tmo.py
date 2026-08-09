Import
SSL
from
datetime import timedelta from functools import cached_property
from typing import Optional, Callable, Awaitable, Any, Happing
Import orjson
from alobreaker import CircuitBreaker, CircuitBreakerListener from tenacity import (
retry_if_exception_type, stop_after _attempt, wait_exponential, AsyncRetrying,
)
from httpx import ( Asyncctient,
Lim1ts，
Timeout, AsynchTPTransport,
Request, AsyncBaseTransport,
Response,
from maestro_ai_core.infrastructure.http.configuration.http_client_configuration import (
HttpolientSettings,
from maestro_ai_core.infrastructure.http.enum.http_method import Httphethod
from maestro_ai_core.infrastructure.http.port.asyne_http_client import AsyncHttpClient
from maestro_ai_core. infrastructure, logger port. logger import Logger
class HttpxClientFactory: 12+ usages
def __init._(
self,
http_client_settings: Optional[HttpCLientSettings] = None,
Logger: Optional(Logger] = None,
http_transport: Optional[AsyncHTTPTransport] = None,
) → None:
self._http_client_settings: HttpCLientSettings = (
http_client_settings or HttpClientSettings)
)
self._logger = logger
self._owns_client: bool = True
self._http_transport = http_transport
async def -_aenter__(self) - "HttpxCLientFactory*:
- = self._cLient_instance
return self
async def —_aexit_(self, exc_type, exc, tb) - None:
await self.close()
async def start(self) -> None:
pass
async def close(self) → None:
if self._owns_client and not self._client_instance.is_closed:
await self._client_instance.aclose()
def create_client(self) -> AsyncHttpClient:
return HttpxClient(self._client_instance)
@cached_property 4+ usages def _client_instance(self) -> AsyncClient:
limits = Limits(
max_connections=self._http_client_settings.Limits.max_connections, max_keepalive_connections=self._http_client_settings.Limits.max_keep_aLive_connections, keepalive_expiry=self._http_client_settings.Limits.keep_alive_timeout,
timeout = Timeout(self._http_client_settings.Limits.timeout)
if not self._http_transport:
self._http_transport = AsyncHTTPTransport(
retries-self._http_client_settings.retry.retry_count, verify=self._create_ssl_from_certificate(),
hooks = ("request": self._event()} if self._event() else None
cLient = AsyncClient(
base_url=self._http_client_settings.client_params.base_url or ",
timeout=timeout, Limits-Limits, transport=self._http_transport, event_hooks=hooks,
return client
@property
def resilient_cLient_instance(self) -> AsyncClient:
return self._resilient_client_instance
@cached_property
1 usage
def
_resilient_client_instance(self) -> AsyncClient:
Limits = Limits(
max_connections=self._http_client_settings.limits.max_connections, max_keepalive_connections=self._http_cLient_settings.Limits.max_keep_alive_connections, keepalive_expiry=self._http_client_settings.Limits.keep_alive_timeout,
timeout = Timeout(self._http_client_settings.Limits.timeout)
if not self._http_transport:
self._http_transport = AsyncHTTPTransport
verify=self._create_ss from_certificateC),
client = AsyncCLient(
base_url=self._nttp_client_settings.client_params.base_urlor •
timeout-timeout, Limits-limits, transport=ResilientTransport(
http_client_settings-self._http_client_settings,
Logger=self._Logger, transport=self._http_transport,
return client
def
_create_ss1_from_certificate(self) -> bool | str | ssl, ssLcontext: 2+ usages
cert_path = self._http_client_settings.security.certificate
if cert_path:
ctx=ssl,create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
ctx.Load_verify_locations(cafilescert_path)
return ctx
return False
def _event(self) - List[Callablel..., Avaitable[Any]1]: 2+ usages
return [self._event_log]
async def _event_log(self, request: Request) -> None: 1 usage
counter: int = 0
if self._logger:
self._logger.info(
f*Executing request frequest.method} -> (request.urL} -- Attempt #{counter}•
class HttpxClient: 1 usage
__slots__ = ("_cLient",)
def _init__(self, client: AsyncClient) -> None:
self._client = client
async def _request 2+ usages
self method: str, endpoint: str
params: Optional[Mapping[str, Any]] = None,
json: optional(Mapping[str, Any]] = None,
headers: Optional[Mapping[str, str]] = None,
) → Any:
response = avait self._client.request(
method-method, urt-endpoint, params-params, Json=json,
headers-headers,
response-raise_for_status (
if "application/json" in response.headers.get("Content-Type", "*):
return orjson. Loads(response.content)
return response, text
async def get( self, endpoint: str,
params: OptionaL[Mapping[str, Any]] = None,
headers: OptionaL[Napping[str, str]] = None,
) →> Any:
return await self._request(HttNethod.GET.value, endpoint, params-params headers=headers)
async def post self, endpoint: str,
body: OptionaL[Mapping(str, Any)) - None,
headers: OptionaL[lapping(str, str = None,
) -> Any:
return await self._request(HttMethod.POST. value, endpoint, json=body, headers=headers)
class BreakerLogger(CircuitBreakerListener): 1 usage
def
-_init__(self, Logger: Optional[Logger] = None):
self._logger = Logger
self._last_exc = None
def failure(self, cb, exc):
self._last_exc = exc
if self._logger:
self._Logger. warning(
fIbreaker] faiture (cb. fail_couhter}/icb.fail_max) recorded: (exc!r)
def success (self, cb):
self._last_exc = None
def state_change(self, cb, old_state, new_state):
if self.Logger:
self._logger. info(f" [breaker] {old_state. state} -> {new_state.statel")
if type(new_state).__name__ == "CircuitopenState":
if self._logger:
self._Logger. error(
f"[breaker] OPEN - (cb.fail_counter} consecutive failures reached f"fail_max={cb.fail_max}. Last error: {self._ last_exc!r).
+ Will allow a trial call again after (cb. timeout_duration}."
class ResilientTransport(AsyncBaseTransport): 1 usage
def __init_(
self,
http_client_settings: Optional[HttpClientSettings] = None,
Logger: Optional[Logger] = None,
transport: AsyncBaseTransport | None = None,
):
self._http_client_settings: HttpClientSettings (
http_client_settings or HttpCLientSettings()
)
self._logger = Logger
self._transport = transport or AsyncHTTPTransportO
self._breaker_logger = Breaker Logger (Logger)
self-_breaker = CircuitBreaker(
fail_max=self._http_client_settings.circuit_breaker.failure_threshold, timeout_duration=timedelta(self._http_client_settings.circuit_breaker.recovery_timeout),
listeners= [self._breaker_logger),
self._retryer = AsyncRetrying(
retrysretry_if_exception_type(self._http_cllent_settings.retry.retry_on),
stop=stop_after_attempt(self._http_client_settings.retry.retry_count),
wait-wait_exponentiall
multiptier=self._http_ctient_settings.retry.retry_delay, min=2, maxes
),
reraisesTrue,
before_sleep-self,_log_retry,
)
def_log_retry(self, retry_state: (outcome)) -> None: 1 usage
exc = retry-state, outcome, exception)
if self._logger:
self_logger, warnings
fIretry) attempt fretry_state,attempt_number) failed • f®(fexcir}); retrying in fretry_state, next_action, sleep:, 1f}s*
async def -retry-execute(self, request: Request) •> Response: 1 usage return await self.-retryer(self._transport.handle_asyno_request,
*args: request)
async def handle_async_requestself, request: Request) -> Response:
return await self._breaker.call_async(self._retry_execute, *args: request)
async def aclose(self):
await self._transport.aclose()

