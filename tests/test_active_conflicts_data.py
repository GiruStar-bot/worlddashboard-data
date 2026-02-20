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
    assert all(isinstance(v, (int, float)) for c in conflicts for v in c["coordinates"])
    assert all(-180 <= c["coordinates"][0] <= 180 for c in conflicts)
    assert all(-90 <= c["coordinates"][1] <= 90 for c in conflicts)
    expected_coordinates = {
        "ukraine_russia_war": [35.0, 48.0],
        "israel_gaza_conflict": [34.5, 31.4],
        "sudan_civil_war": [32.5, 15.5],
        "myanmar_civil_war": [96.0, 20.0],
        "yemen_houthi_conflict": [44.0, 15.0],
        "sample_ceasefire": [46.7, 39.8],
    }
    assert {c["id"]: c["coordinates"] for c in conflicts} == expected_coordinates
