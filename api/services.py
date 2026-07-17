"""Application services for the Claude-compatible API."""

from __future__ import annotations

import asyncio
import json
import time
import traceback
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from loguru import logger

from config.settings import Settings
from core.anthropic import (
    append_request_id,
    get_token_count,
    get_user_facing_error_message,
    iter_provider_stream_error_sse_events,
)
from core.anthropic.sse import ANTHROPIC_SSE_RESPONSE_HEADERS
from core.trace import api_messages_request_snapshot, trace_event, traced_async_stream
from providers.base import BaseProvider
from providers.exceptions import InvalidRequestError, ProviderError

from . import evo_router, metrics
from .model_router import ModelRouter, RoutedMessagesRequest
from .models.anthropic import MessagesRequest, TokenCountRequest
from .models.responses import TokenCountResponse
from .optimization_handlers import try_optimizations
from .web_tools.egress import WebFetchEgressPolicy
from .web_tools.request import (
    is_web_server_tool_request,
    openai_chat_upstream_server_tool_error,
)
from .web_tools.streaming import stream_web_server_tool_response

TokenCounter = Callable[[list[Any], str | list[Any] | None, list[Any] | None], int]

ProviderGetter = Callable[[str], BaseProvider]

# Providers that use ``/chat/completions`` + Anthropic-to-OpenAI conversion (not native Messages).
_OPENAI_CHAT_UPSTREAM_IDS = frozenset({"nvidia_nim", "opencode", "opencode_go"})


@dataclass(frozen=True, slots=True)
class _RouteAttempt:
    """One provider/model candidate in the (possibly single-entry) failover chain."""

    request: MessagesRequest
    provider_id: str
    provider_model: str
    model_ref: str
    thinking_enabled: bool


def anthropic_sse_streaming_response(
    body: AsyncIterator[str],
) -> StreamingResponse:
    """Return a :class:`StreamingResponse` for Anthropic-style SSE streams."""
    return StreamingResponse(
        body,
        media_type="text/event-stream",
        headers=ANTHROPIC_SSE_RESPONSE_HEADERS,
    )


async def _metered_stream(
    agen: AsyncIterator[str],
    *,
    request_id: str,
    provider_id: str,
    model: str,
    input_tokens: int,
    model_ref: str,
) -> AsyncIterator[str]:
    """Wrap a provider SSE stream to record metrics + evo-router fitness."""
    start = time.perf_counter()
    output_tokens = 0
    status = "ok"
    fitness_error: str | None = None
    record_fitness = True
    try:
        async for chunk in agen:
            if '"message_delta"' in chunk and '"output_tokens"' in chunk:
                for line in chunk.splitlines():
                    if line.startswith("data: "):
                        try:
                            obj = json.loads(line[6:])
                            if obj.get("type") == "message_delta":
                                tok = obj.get("usage", {}).get("output_tokens")
                                if isinstance(tok, int):
                                    output_tokens = tok
                        except Exception:
                            pass
            yield chunk
    except ProviderError as exc:
        status = "error"
        fitness_error = exc.error_type
        raise
    except GeneratorExit, asyncio.CancelledError:
        # Client cancelled — an error for metrics, but not the provider's fault.
        status = "error"
        record_fitness = False
        raise
    except BaseException:
        status = "error"
        fitness_error = "unexpected"
        raise
    finally:
        latency_ms = (time.perf_counter() - start) * 1000
        metrics.record(
            request_id=request_id,
            provider_id=provider_id,
            model=model,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            latency_ms=latency_ms,
            status=status,
        )
        if record_fitness:
            evo_router.record_outcome(
                model_ref,
                ok=status == "ok",
                latency_ms=latency_ms,
                error_type=fitness_error,
            )


async def _stream_with_failover(
    attempts: list[_RouteAttempt],
    *,
    provider_getter: ProviderGetter,
    request_id: str,
    input_tokens: int,
    settings: Settings,
    original_gateway_model: str,
) -> AsyncIterator[str]:
    """Yield from the first attempt whose stream opens without a pre-stream error.

    Each attempt's provider raises :class:`ProviderError` on the first
    ``__anext__()`` when ``escalate_pre_stream_errors=True`` (set implicitly
    when ``FAILOVER_MODELS`` is configured). On such an error we record a
    fitness outcome for the attempt and try the next candidate. When all
    attempts fail, we emit a well-formed Anthropic error SSE stream so the
    client sees a message lifecycle instead of a truncated connection.
    """
    last_exc: ProviderError | None = None
    last_provider_name: str = attempts[0].provider_id if attempts else ""
    for attempt in attempts:
        provider = provider_getter(attempt.provider_id)
        try:
            provider.preflight_stream(
                attempt.request,
                thinking_enabled=attempt.thinking_enabled,
            )
        except ProviderError as exc:
            last_exc = exc
            last_provider_name = attempt.provider_id
            evo_router.record_outcome(
                attempt.model_ref,
                ok=False,
                latency_ms=0.0,
                error_type=exc.error_type,
            )
            continue

        agen = provider.stream_response(
            attempt.request,
            input_tokens=input_tokens,
            request_id=request_id,
            thinking_enabled=attempt.thinking_enabled,
        )
        start = time.perf_counter()
        try:
            first_chunk = await agen.__anext__()
        except StopAsyncIteration:
            latency_ms = (time.perf_counter() - start) * 1000
            evo_router.record_outcome(
                attempt.model_ref,
                ok=False,
                latency_ms=latency_ms,
                error_type="empty_stream",
            )
            last_exc = ProviderError("Empty response stream")
            last_provider_name = attempt.provider_id
            continue
        except ProviderError as exc:
            latency_ms = (time.perf_counter() - start) * 1000
            evo_router.record_outcome(
                attempt.model_ref,
                ok=False,
                latency_ms=latency_ms,
                error_type=exc.error_type,
            )
            last_exc = exc
            last_provider_name = attempt.provider_id
            trace_event(
                stage="routing",
                event="api.failover.attempt_failed",
                source="api",
                request_id=request_id,
                provider_id=attempt.provider_id,
                model_ref=attempt.model_ref,
                error_type=exc.error_type,
            )
            continue

        trace_event(
            stage="routing",
            event="api.failover.attempt_won",
            source="api",
            request_id=request_id,
            provider_id=attempt.provider_id,
            model_ref=attempt.model_ref,
        )

        async def _replay_winning_stream(
            first: str = first_chunk,
            tail: AsyncIterator[str] = agen,
        ) -> AsyncIterator[str]:
            yield first
            async for chunk in tail:
                yield chunk

        traced = traced_async_stream(
            _replay_winning_stream(),
            stage="egress",
            source="api",
            complete_event="api.response.stream_completed",
            interrupted_event="api.response.stream_interrupted",
            chunk_event=None,
            extra={
                "request_id": request_id,
                "provider_id": attempt.provider_id,
                "gateway_model": original_gateway_model,
            },
        )
        async for chunk in _metered_stream(
            traced,
            request_id=request_id,
            provider_id=attempt.provider_id,
            model=attempt.request.model,
            input_tokens=input_tokens,
            model_ref=attempt.model_ref,
        ):
            yield chunk
        return

    # Every attempt failed: emit an in-band SSE error using the last error.
    if last_exc is None:
        last_exc = ProviderError("No routing candidates")
    base_message = (
        f"All failover candidates failed (last provider: {last_provider_name}): "
        f"{last_exc}"
    )
    error_message = append_request_id(base_message, request_id)
    stub_request = attempts[0].request if attempts else None
    for event in iter_provider_stream_error_sse_events(
        request=stub_request,
        input_tokens=input_tokens,
        error_message=error_message,
        sent_any_event=False,
        log_raw_sse_events=settings.log_raw_sse_events,
    ):
        yield event


def _http_status_for_unexpected_service_exception(_exc: BaseException) -> int:
    """HTTP status for uncaught non-provider failures (stable client contract)."""
    return 500


def _log_unexpected_service_exception(
    settings: Settings,
    exc: BaseException,
    *,
    context: str,
    request_id: str | None = None,
) -> None:
    """Log service-layer failures without echoing exception text unless opted in."""
    if settings.log_api_error_tracebacks:
        if request_id is not None:
            logger.error("{} request_id={}: {}", context, request_id, exc)
        else:
            logger.error("{}: {}", context, exc)
        logger.error(traceback.format_exc())
        return
    if request_id is not None:
        logger.error(
            "{} request_id={} exc_type={}",
            context,
            request_id,
            type(exc).__name__,
        )
    else:
        logger.error("{} exc_type={}", context, type(exc).__name__)


def _require_non_empty_messages(messages: list[Any]) -> None:
    if not messages:
        raise InvalidRequestError("messages cannot be empty")


class ClaudeProxyService:
    """Coordinate request optimization, model routing, token count, and providers."""

    def __init__(
        self,
        settings: Settings,
        provider_getter: ProviderGetter,
        model_router: ModelRouter | None = None,
        token_counter: TokenCounter = get_token_count,
    ):
        self._settings = settings
        self._provider_getter = provider_getter
        self._model_router = model_router or ModelRouter(settings)
        self._token_counter = token_counter

    def _build_route_attempts(
        self,
        primary: RoutedMessagesRequest,
        original: MessagesRequest,
    ) -> list[_RouteAttempt]:
        """Return the failover chain ordered by evo-router if enabled.

        The chain is the resolved primary + every distinct ``FAILOVER_MODELS``
        ref that :class:`ModelRouter` can resolve. A single-entry list is the
        no-failover case.
        """
        by_ref: dict[str, _RouteAttempt] = {}
        by_ref[primary.resolved.provider_model_ref] = _RouteAttempt(
            request=primary.request,
            provider_id=primary.resolved.provider_id,
            provider_model=primary.resolved.provider_model,
            model_ref=primary.resolved.provider_model_ref,
            thinking_enabled=primary.resolved.thinking_enabled,
        )
        for ref in self._settings.failover_model_refs():
            if ref in by_ref:
                continue
            try:
                alt = self._model_router.resolve_messages_request(
                    original.model_copy(update={"model": ref}, deep=True),
                )
            except Exception:
                continue
            by_ref[alt.resolved.provider_model_ref] = _RouteAttempt(
                request=alt.request,
                provider_id=alt.resolved.provider_id,
                provider_model=alt.resolved.provider_model,
                model_ref=alt.resolved.provider_model_ref,
                thinking_enabled=alt.resolved.thinking_enabled,
            )
        ranked = evo_router.rank(
            list(by_ref.keys()),
            evo=self._settings.evo_routing,
        )
        return [by_ref[r] for r in ranked]

    def create_message(self, request_data: MessagesRequest) -> object:
        """Create a message response or streaming response."""
        try:
            _require_non_empty_messages(request_data.messages)

            routed = self._model_router.resolve_messages_request(request_data)
            if routed.resolved.provider_id in _OPENAI_CHAT_UPSTREAM_IDS:
                tool_err = openai_chat_upstream_server_tool_error(
                    routed.request,
                    web_tools_enabled=self._settings.enable_web_server_tools,
                )
                if tool_err is not None:
                    raise InvalidRequestError(tool_err)

            if self._settings.enable_web_server_tools and is_web_server_tool_request(
                routed.request
            ):
                input_tokens = self._token_counter(
                    routed.request.messages, routed.request.system, routed.request.tools
                )
                trace_event(
                    stage="routing",
                    event="api.optimization.web_server_tool",
                    source="api",
                    model=routed.request.model,
                )
                egress = WebFetchEgressPolicy(
                    allow_private_network_targets=self._settings.web_fetch_allow_private_networks,
                    allowed_schemes=self._settings.web_fetch_allowed_scheme_set(),
                )
                return anthropic_sse_streaming_response(
                    stream_web_server_tool_response(
                        routed.request,
                        input_tokens=input_tokens,
                        web_fetch_egress=egress,
                        verbose_client_errors=self._settings.log_api_error_tracebacks,
                    ),
                )

            optimized = try_optimizations(routed.request, self._settings)
            if optimized is not None:
                trace_event(
                    stage="routing",
                    event="api.optimization.short_circuit",
                    source="api",
                    model=routed.request.model,
                )
                return optimized
            logger.debug("No optimization matched, routing to provider")

            failover_on = self._settings.failover_enabled
            if failover_on:
                attempts = self._build_route_attempts(routed, request_data)
                failover_chain = [a.model_ref for a in attempts]
            else:
                attempts = []
                failover_chain = []
                provider = self._provider_getter(routed.resolved.provider_id)
                provider.preflight_stream(
                    routed.request,
                    thinking_enabled=routed.resolved.thinking_enabled,
                )

            trace_event(
                stage="routing",
                event="api.route.resolved",
                source="api",
                provider_id=routed.resolved.provider_id,
                provider_model=routed.resolved.provider_model,
                provider_model_ref=routed.resolved.provider_model_ref,
                gateway_model=routed.request.model,
                thinking_enabled=routed.resolved.thinking_enabled,
                failover_chain=failover_chain,
            )

            request_id = f"req_{uuid.uuid4().hex[:12]}"
            with logger.contextualize(request_id=request_id):
                trace_event(
                    stage="ingress",
                    event="api.request.received",
                    source="api",
                    message_count=len(routed.request.messages),
                    snapshot=api_messages_request_snapshot(routed.request),
                )

                if self._settings.log_raw_api_payloads:
                    logger.debug(
                        "FULL_PAYLOAD [{}]: {}", request_id, routed.request.model_dump()
                    )

                input_tokens = self._token_counter(
                    routed.request.messages,
                    routed.request.system,
                    routed.request.tools,
                )

                if failover_on:
                    stream = _stream_with_failover(
                        attempts,
                        provider_getter=self._provider_getter,
                        request_id=request_id,
                        input_tokens=input_tokens,
                        settings=self._settings,
                        original_gateway_model=routed.request.model,
                    )
                    return anthropic_sse_streaming_response(stream)

                streamed = traced_async_stream(
                    provider.stream_response(
                        routed.request,
                        input_tokens=input_tokens,
                        request_id=request_id,
                        thinking_enabled=routed.resolved.thinking_enabled,
                    ),
                    stage="egress",
                    source="api",
                    complete_event="api.response.stream_completed",
                    interrupted_event="api.response.stream_interrupted",
                    chunk_event=None,
                    extra={
                        "request_id": request_id,
                        "provider_id": routed.resolved.provider_id,
                        "gateway_model": routed.request.model,
                    },
                )
                metered = _metered_stream(
                    streamed,
                    request_id=request_id,
                    provider_id=routed.resolved.provider_id,
                    model=routed.request.model,
                    input_tokens=input_tokens,
                    model_ref=routed.resolved.provider_model_ref,
                )
                return anthropic_sse_streaming_response(metered)

        except ProviderError:
            raise
        except Exception as e:
            _log_unexpected_service_exception(
                self._settings, e, context="CREATE_MESSAGE_ERROR"
            )
            raise HTTPException(
                status_code=_http_status_for_unexpected_service_exception(e),
                detail=get_user_facing_error_message(e),
            ) from e

    def count_tokens(self, request_data: TokenCountRequest) -> TokenCountResponse:
        """Count tokens for a request after applying configured model routing."""
        request_id = f"req_{uuid.uuid4().hex[:12]}"
        with logger.contextualize(request_id=request_id):
            try:
                _require_non_empty_messages(request_data.messages)
                routed = self._model_router.resolve_token_count_request(request_data)
                tokens = self._token_counter(
                    routed.request.messages, routed.request.system, routed.request.tools
                )
                trace_event(
                    stage="routing",
                    event="api.route.resolved",
                    source="api",
                    kind="count_tokens",
                    provider_id=routed.resolved.provider_id,
                    provider_model=routed.resolved.provider_model,
                    provider_model_ref=routed.resolved.provider_model_ref,
                    gateway_model=routed.request.model,
                )
                trace_event(
                    stage="ingress",
                    event="api.count_tokens.completed",
                    source="api",
                    message_count=len(routed.request.messages),
                    input_tokens=tokens,
                    snapshot=api_messages_request_snapshot(routed.request),
                )
                return TokenCountResponse(input_tokens=tokens)
            except ProviderError:
                raise
            except Exception as e:
                _log_unexpected_service_exception(
                    self._settings,
                    e,
                    context="COUNT_TOKENS_ERROR",
                    request_id=request_id,
                )
                raise HTTPException(
                    status_code=_http_status_for_unexpected_service_exception(e),
                    detail=get_user_facing_error_message(e),
                ) from e
