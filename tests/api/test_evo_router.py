"""Unit tests for :mod:`api.evo_router` (fitness, cooldown, ranking)."""

from __future__ import annotations

import time

import pytest

from api import evo_router


@pytest.fixture(autouse=True)
def _reset_store():
    evo_router.clear()
    yield
    evo_router.clear()


def test_unknown_ref_returns_neutral_prior():
    assert evo_router.fitness("openai/gpt-4o") == pytest.approx(0.6)


def test_all_ok_yields_high_fitness():
    for _ in range(5):
        evo_router.record_outcome("openai/gpt-4o", ok=True, latency_ms=100.0)
    assert evo_router.fitness("openai/gpt-4o") > 0.95


def test_all_failed_yields_zero_success_fitness():
    for _ in range(5):
        evo_router.record_outcome(
            "openai/gpt-4o", ok=False, latency_ms=0.0, error_type="rate_limit"
        )
    assert evo_router.fitness("openai/gpt-4o") == pytest.approx(0.0)


def test_recent_outcomes_dominate():
    for _ in range(20):
        evo_router.record_outcome("m", ok=False, latency_ms=0.0)
    early_fitness = evo_router.fitness("m")
    for _ in range(3):
        evo_router.record_outcome("m", ok=True, latency_ms=100.0)
    assert evo_router.fitness("m") > early_fitness + 0.2


def test_latency_penalty_reduces_fitness():
    for _ in range(5):
        evo_router.record_outcome("slow", ok=True, latency_ms=50_000.0)
    for _ in range(5):
        evo_router.record_outcome("fast", ok=True, latency_ms=100.0)
    assert evo_router.fitness("fast") > evo_router.fitness("slow")


def test_cooldown_activates_after_consecutive_failures():
    ref = "flaky/model"
    for _ in range(3):
        evo_router.record_outcome(ref, ok=False, latency_ms=0.0)
    assert evo_router.in_cooldown(ref) is True


def test_cooldown_clears_after_success():
    ref = "recovered/model"
    for _ in range(3):
        evo_router.record_outcome(ref, ok=False, latency_ms=0.0)
    evo_router.record_outcome(ref, ok=True, latency_ms=100.0)
    assert evo_router.in_cooldown(ref) is False


def test_cooldown_expires_by_time_window():
    ref = "expired/model"
    for _ in range(3):
        evo_router.record_outcome(ref, ok=False, latency_ms=0.0)
    assert evo_router.in_cooldown(ref, now=time.time() + 3600) is False


def test_rank_static_preserves_order():
    # Two ok outcomes for a, five ok for b — b is fitter but static rank
    # keeps configured order.
    for _ in range(2):
        evo_router.record_outcome("a", ok=True, latency_ms=500.0)
    for _ in range(5):
        evo_router.record_outcome("b", ok=True, latency_ms=100.0)
    ranked = evo_router.rank(["a", "b"], evo=False)
    assert ranked == ["a", "b"]


def test_rank_evo_orders_by_fitness():
    for _ in range(5):
        evo_router.record_outcome("a", ok=True, latency_ms=50_000.0)
    for _ in range(5):
        evo_router.record_outcome("b", ok=True, latency_ms=100.0)
    ranked = evo_router.rank(["a", "b"], evo=True)
    assert ranked == ["b", "a"]


def test_rank_moves_cooled_refs_to_back():
    for _ in range(3):
        evo_router.record_outcome("cooled", ok=False, latency_ms=0.0)
    ranked = evo_router.rank(["cooled", "fresh"], evo=False)
    assert ranked == ["fresh", "cooled"]


def test_rank_dedupes_candidates():
    ranked = evo_router.rank(["x", "y", "x"], evo=False)
    assert ranked == ["x", "y"]


def test_snapshot_reports_state():
    evo_router.record_outcome("m", ok=True, latency_ms=100.0)
    evo_router.record_outcome("m", ok=False, latency_ms=200.0, error_type="timeout")
    snap = evo_router.snapshot()["m"]
    assert snap["outcomes"] == 2
    assert snap["consecutive_failures"] == 1
    assert snap["last_error_type"] == "timeout"
    assert snap["last_ok"] is False
