"""Integration test for the failover loop in :mod:`api.services`."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from api import evo_router
from api.models.anthropic import MessagesRequest
from api.services import _RouteAttempt, _stream_with_failover
from config.settings import Settings
from providers.base import BaseProvider, ProviderConfig
from providers.exceptions import ProviderError


class _StubProvider(BaseProvider):
    """Minimal :class:`BaseProvider` used to script pass/fail streaming outcomes."""

    def __init__(self, tag: str, mode: str) -> None:
        super().__init__(ProviderConfig(api_key="test"))
        self.tag = tag
        self.mode = mode

    async def cleanup(self) -> None:
        return None

    async def list_model_ids(self) -> frozenset[str]:
        return frozenset({"test-model"})

    def preflight_stream(
        self, request: Any, *, thinking_enabled: bool | None = None
    ) -> None:
        return None

    async def stream_response(
        self,
        request: Any,
        input_tokens: int = 0,
        *,
        request_id: str | None = None,
        thinking_enabled: bool | None = None,
    ) -> AsyncIterator[str]:
        if self.mode == "raise":
            raise ProviderError(
                f"synthetic {self.tag} failure", error_type="rate_limit"
            )
        yield 'event: message_start\ndata: {"type":"message_start"}\n\n'
        yield (
            "event: message_delta\n"
            'data: {"type":"message_delta","usage":{"output_tokens":7}}\n\n'
        )


def _request() -> MessagesRequest:
    return MessagesRequest(
        model="test-model",
        messages=[{"role": "user", "content": "hi"}],
        max_tokens=10,
    )


def _attempt(provider_id: str, model_ref: str) -> _RouteAttempt:
    return _RouteAttempt(
        request=_request(),
        provider_id=provider_id,
        provider_model="test-model",
        model_ref=model_ref,
        thinking_enabled=False,
    )


@pytest.fixture(autouse=True)
def _reset_evo_store():
    evo_router.clear()
    yield
    evo_router.clear()


@pytest.fixture
def _settings():
    return Settings()


@pytest.mark.asyncio
async def test_failover_falls_through_to_second_candidate_on_provider_error(_settings):
    stubs: dict[str, BaseProvider] = {
        "openai": _StubProvider("openai", "raise"),
        "anthropic": _StubProvider("anthropic", "ok"),
    }
    attempts = [
        _attempt("openai", "openai/gpt-4o"),
        _attempt("anthropic", "anthropic/claude"),
    ]
    chunks = [
        chunk
        async for chunk in _stream_with_failover(
            attempts,
            provider_getter=lambda pid: stubs[pid],
            request_id="req_x",
            input_tokens=5,
            settings=_settings,
            original_gateway_model="test-model",
        )
    ]

    assert any("message_start" in c for c in chunks)
    assert any("message_delta" in c for c in chunks)

    snap = evo_router.snapshot()
    assert snap["openai/gpt-4o"]["last_ok"] is False
    assert snap["openai/gpt-4o"]["last_error_type"] == "rate_limit"
    assert snap["anthropic/claude"]["last_ok"] is True


@pytest.mark.asyncio
async def test_failover_emits_error_sse_when_all_candidates_fail(_settings):
    stubs: dict[str, BaseProvider] = {
        "openai": _StubProvider("openai", "raise"),
        "anthropic": _StubProvider("anthropic", "raise"),
    }
    attempts = [
        _attempt("openai", "openai/gpt-4o"),
        _attempt("anthropic", "anthropic/claude"),
    ]
    chunks = [
        chunk
        async for chunk in _stream_with_failover(
            attempts,
            provider_getter=lambda pid: stubs[pid],
            request_id="req_y",
            input_tokens=0,
            settings=_settings,
            original_gateway_model="test-model",
        )
    ]

    joined = "".join(chunks)
    assert "All failover candidates failed" in joined
    assert "message_stop" in joined

    snap = evo_router.snapshot()
    assert snap["openai/gpt-4o"]["last_ok"] is False
    assert snap["anthropic/claude"]["last_ok"] is False


@pytest.mark.asyncio
async def test_single_candidate_chain_streams_successfully(_settings):
    stubs: dict[str, BaseProvider] = {"openai": _StubProvider("openai", "ok")}
    attempts = [_attempt("openai", "openai/gpt-4o")]
    chunks = [
        chunk
        async for chunk in _stream_with_failover(
            attempts,
            provider_getter=lambda pid: stubs[pid],
            request_id="req_z",
            input_tokens=0,
            settings=_settings,
            original_gateway_model="test-model",
        )
    ]

    assert any("message_start" in c for c in chunks)
    assert evo_router.snapshot()["openai/gpt-4o"]["last_ok"] is True


def test_failover_model_refs_parses_and_dedupes(monkeypatch):
    monkeypatch.setenv(
        "FAILOVER_MODELS",
        " openai/gpt-4o , open_router/anthropic/claude-3 , openai/gpt-4o ",
    )
    monkeypatch.setenv("MODEL", "nvidia_nim/test-model")
    settings = Settings()
    assert settings.failover_model_refs() == (
        "openai/gpt-4o",
        "open_router/anthropic/claude-3",
    )
    assert settings.failover_enabled is True


def test_failover_disabled_by_default(monkeypatch):
    monkeypatch.delenv("FAILOVER_MODELS", raising=False)
    monkeypatch.setenv("MODEL", "nvidia_nim/test-model")
    settings = Settings()
    assert settings.failover_enabled is False
    assert settings.failover_model_refs() == ()


def test_failover_models_rejects_unknown_provider(monkeypatch):
    monkeypatch.setenv("FAILOVER_MODELS", "unknown_provider/some-model")
    monkeypatch.setenv("MODEL", "nvidia_nim/test-model")
    with pytest.raises(ValueError, match="Invalid FAILOVER_MODELS"):
        Settings()
