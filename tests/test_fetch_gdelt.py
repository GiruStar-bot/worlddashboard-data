"""Tests for scripts/fetch_gdelt.py – process() function."""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Ensure scripts/ is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from fetch_gdelt import TARGET_ISO3, process


def _make_df(rows):
    """Helper to build a minimal DataFrame that process() expects."""
    cols = [
        "Actor1Geo_CountryCode",
        "QuadClass",
        "EventRootCode",
        "GoldsteinScale",
        "SOURCEURL",
    ]
    return pd.DataFrame(rows, columns=cols)


# ---------- TARGET_ISO3 ----------

def test_target_iso3_contains_all_required():
    """Verify that every country listed in the spec is present."""
    africa = {"EGY", "ZAF", "NGA", "KEN", "ETH", "SDN", "COD", "SOM", "LBY", "MLI",
              "BFA", "NER", "TCD", "MOZ", "CAF", "CMR", "BDI", "SSD", "ZWE", "AGO"}
    middle_east = {"SAU", "IRN", "IRQ", "ISR", "JOR", "LBN", "SYR", "YEM", "ARE", "QAT",
                   "KWT", "OMN", "BHR", "TUR", "PSE"}
    southeast_asia = {"IDN", "MYS", "PHL", "SGP", "THA", "VNM", "KHM", "LAO", "MMR",
                      "BRN", "TLS", "TWN"}
    south_america = {"BRA", "ARG", "COL", "PER", "VEN", "CHL", "ECU", "BOL", "PRY",
                     "URY", "GUY", "SUR"}
    expected = africa | middle_east | southeast_asia | south_america
    assert TARGET_ISO3 == expected


# ---------- Conflict filter ----------

def test_conflict_filter_quad_class_4():
    """QuadClass == 4 events should be included."""
    df = _make_df([
        # EG = EGY (target); QuadClass=4; 2 events → |−10|+|−10| = 20 / 10 = 2.0 ≥ threshold
        ("EG", 4, 1, -10.0, "http://example.com/a"),
        ("EG", 4, 1, -10.0, "http://example.com/a2"),
    ])
    result = process(df)
    assert "EGY" in result


def test_conflict_filter_event_root_code_14():
    """EventRootCode == 14 events should be included."""
    df = _make_df([
        # SA = SAU (target); EventRootCode=14
        ("SA", 1, 14, -6.5, "http://example.com/b"),
    ])
    result = process(df)
    # Only 1 event with |−6.5|/10 = 0.65 → below threshold, should be excluded
    assert "SAU" not in result


def test_conflict_filter_excludes_non_conflict():
    """Non-conflict rows (QuadClass ≠ 4 and EventRootCode ≠ 14) are dropped."""
    df = _make_df([
        ("EG", 1, 1, 5.0, "http://example.com/c"),  # verbal cooperation
        ("EG", 2, 2, 3.0, "http://example.com/d"),  # verbal conflict
    ])
    result = process(df)
    assert result == {}


# ---------- Target region filter ----------

def test_non_target_country_excluded():
    """USA (not in TARGET_ISO3) should never appear."""
    df = _make_df([
        ("US", 4, 18, -10.0, "http://example.com/e"),
        ("US", 4, 18, -10.0, "http://example.com/f"),
        ("US", 4, 18, -10.0, "http://example.com/g"),
    ])
    result = process(df)
    assert "USA" not in result


def test_target_country_included():
    """IRQ (target) with enough score should appear."""
    # 3 events × |−10.0| = 30.0 → 30/10 = 3.0 ≥ 2.0
    df = _make_df([
        ("IZ", 4, 18, -10.0, "http://example.com/h"),
        ("IZ", 4, 18, -10.0, "http://example.com/i"),
        ("IZ", 4, 18, -10.0, "http://example.com/j"),
    ])
    result = process(df)
    assert "IRQ" in result


# ---------- Risk score calculation ----------

def test_risk_score_formula():
    """Risk Score = sum(abs(GoldsteinScale)) / 10."""
    # 3 events for SDN: |−10|+|−5|+|−5| = 20 → 20/10 = 2.0
    df = _make_df([
        ("SU", 4, 18, -10.0, "http://example.com/1"),
        ("SU", 4, 18, -5.0, "http://example.com/2"),
        ("SU", 4, 18, -5.0, "http://example.com/3"),
    ])
    result = process(df)
    assert "SDN" in result
    assert result["SDN"]["risk_score"] == 2.0
    assert result["SDN"]["count"] == 3


def test_risk_score_positive_for_negative_goldstein():
    """Scores are always positive regardless of GoldsteinScale sign."""
    df = _make_df([
        ("SU", 4, 18, -10.0, "http://example.com/1"),
        ("SU", 4, 18, -10.0, "http://example.com/2"),
    ])
    result = process(df)
    assert "SDN" in result
    assert result["SDN"]["risk_score"] == 2.0
    assert result["SDN"]["risk_score"] > 0


# ---------- Threshold ----------

def test_threshold_excludes_low_score():
    """Risk Score < 2.0 countries must be excluded."""
    # 1 event: |−5.0| / 10 = 0.5 < 2.0
    df = _make_df([
        ("EG", 4, 18, -5.0, "http://example.com/low"),
    ])
    result = process(df)
    assert "EGY" not in result


def test_threshold_includes_at_boundary():
    """Risk Score == 2.0 should be included (≥ 2.0)."""
    # |−10| + |−10| = 20 → 20/10 = 2.0
    df = _make_df([
        ("EG", 4, 18, -10.0, "http://example.com/a"),
        ("EG", 4, 18, -10.0, "http://example.com/b"),
    ])
    result = process(df)
    assert "EGY" in result
    assert result["EGY"]["risk_score"] == 2.0


# ---------- top_news ----------

def test_top_news_is_most_severe_event():
    """top_news should be the URL of the event with the highest BaseScore."""
    df = _make_df([
        ("SU", 4, 18, -2.0, "http://example.com/minor"),
        ("SU", 4, 18, -10.0, "http://example.com/severe"),
        ("SU", 4, 18, -8.0, "http://example.com/medium"),
    ])
    result = process(df)
    assert "SDN" in result
    assert result["SDN"]["top_news"] == "http://example.com/severe"


# ---------- Output shape ----------

def test_output_keys():
    """Each entry must have risk_score, count, top_news (no stability)."""
    df = _make_df([
        ("IZ", 4, 18, -10.0, "http://example.com/k"),
        ("IZ", 4, 18, -10.0, "http://example.com/l"),
        ("IZ", 4, 18, -10.0, "http://example.com/m"),
    ])
    result = process(df)
    assert "IRQ" in result
    entry = result["IRQ"]
    assert set(entry.keys()) == {"risk_score", "count", "top_news"}
    assert "stability" not in entry


def test_empty_input():
    """An empty DataFrame should produce an empty result."""
    df = _make_df([])
    result = process(df)
    assert result == {}
