"""Tests for scripts/run_kodoku_engine.py – KODOKU Engine V2 context verification."""

import sys
from pathlib import Path

import pytest

# Ensure scripts/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from run_kodoku_engine import (
    HISTORICAL_CRISES,
    MARITIME_KEYWORDS,
    compute_context_multiplier,
    compute_chokepoint_disruption,
    haversine_km,
)


# ---------- HISTORICAL_CRISES definition ----------

def test_historical_crises_has_required_keys():
    """Verify HISTORICAL_CRISES contains both fingerprints."""
    assert "suez_blockade" in HISTORICAL_CRISES
    assert "internal_unrest" in HISTORICAL_CRISES


def test_suez_blockade_weight():
    assert HISTORICAL_CRISES["suez_blockade"]["weight"] == 2.0


def test_internal_unrest_weight():
    assert HISTORICAL_CRISES["internal_unrest"]["weight"] == 0.1


def test_maritime_keywords_defined():
    assert "canal" in MARITIME_KEYWORDS
    assert "strait" in MARITIME_KEYWORDS
    assert "missile" in MARITIME_KEYWORDS


# ---------- compute_context_multiplier ----------

def test_context_multiplier_internal_protest_no_maritime():
    """Majority protests + no maritime keywords → 0.1 (attenuate)."""
    country = {
        "risk_score": 10.0,
        "event_codes": {"14": 8, "1": 2},
        "keywords": ["protest", "election", "police"],
    }
    assert compute_context_multiplier(country) == pytest.approx(0.1)


def test_context_multiplier_military_with_maritime():
    """Military events + maritime keywords → 2.0 (amplify)."""
    country = {
        "risk_score": 10.0,
        "event_codes": {"19": 5, "14": 2},
        "keywords": ["missile", "tanker", "attack"],
    }
    assert compute_context_multiplier(country) == pytest.approx(2.0)


def test_context_multiplier_default():
    """Mixed events without clear pattern → 1.0 (no change)."""
    country = {
        "risk_score": 5.0,
        "event_codes": {"18": 5, "14": 3},
        "keywords": ["border", "conflict", "region"],
    }
    assert compute_context_multiplier(country) == pytest.approx(1.0)


def test_context_multiplier_empty_event_codes():
    """No event codes → 1.0."""
    country = {"risk_score": 5.0, "event_codes": {}, "keywords": []}
    assert compute_context_multiplier(country) == pytest.approx(1.0)


def test_context_multiplier_missing_event_codes():
    """Missing event_codes key → 1.0 (backward compat)."""
    country = {"risk_score": 5.0}
    assert compute_context_multiplier(country) == pytest.approx(1.0)


def test_context_multiplier_protest_with_maritime_not_attenuated():
    """Protests but WITH maritime keywords should NOT be attenuated."""
    country = {
        "risk_score": 10.0,
        "event_codes": {"14": 9, "1": 1},
        "keywords": ["protest", "canal", "blocked"],
    }
    # Has maritime keywords, so not attenuated even though mostly protests
    assert compute_context_multiplier(country) != pytest.approx(0.1)


# ---------- compute_chokepoint_disruption with context ----------

def test_disruption_attenuated_for_protest():
    """Internal protest data should produce lower disruption risk."""
    # ISR is near Suez Canal; simulate a protest-dominated scenario
    risk_data_protest = {
        "ISR": {
            "risk_score": 12.0,
            "count": 10,
            "top_news": "",
            "event_codes": {"14": 9, "1": 1},
            "keywords": ["protest", "demonstration", "police"],
        },
    }
    risk_data_military = {
        "ISR": {
            "risk_score": 12.0,
            "count": 10,
            "top_news": "",
            "event_codes": {"19": 5, "14": 2},
            "keywords": ["missile", "tanker", "strait"],
        },
    }
    result_protest = compute_chokepoint_disruption(risk_data_protest)
    result_military = compute_chokepoint_disruption(risk_data_military)

    # Protest scenario should have much lower Suez disruption than military
    assert result_protest["suez"]["disruption_risk"] < result_military["suez"]["disruption_risk"]


def test_disruption_with_legacy_data():
    """Legacy data without event_codes/keywords should still work (multiplier=1.0)."""
    risk_data = {
        "IRN": {"risk_score": 6.0, "count": 5, "top_news": ""},
    }
    result = compute_chokepoint_disruption(risk_data)
    assert "hormuz" in result
    assert result["hormuz"]["disruption_risk"] >= 0
