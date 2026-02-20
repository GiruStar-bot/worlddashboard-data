import json
from pathlib import Path


def test_active_conflicts_data_shape_and_values():
    data_path = Path(__file__).resolve().parent.parent / "public" / "data" / "active_conflicts.json"
    conflicts = json.loads(data_path.read_text(encoding="utf-8"))

    assert len(conflicts) == 6
    assert [c["id"] for c in conflicts] == [
        "ukraine_russia_war",
        "israel_gaza_conflict",
        "sudan_civil_war",
        "myanmar_civil_war",
        "yemen_houthi_conflict",
        "sample_ceasefire",
    ]
    assert conflicts[-1]["status"] == "ceasefire"
    assert all(c["status"] in {"active", "ceasefire"} for c in conflicts)
    assert all(len(c["coordinates"]) == 2 for c in conflicts)
