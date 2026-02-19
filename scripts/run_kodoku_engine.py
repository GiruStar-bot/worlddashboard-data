"""
run_kodoku_engine.py
Project KODOKU - Global Supply-Chain Survival Probability Simulator

GDELT の紛争データとチョークポイントの地理情報を組み合わせ、
主要海運ルート（シーレーン）の封鎖確率・生存確率を数学的に算出する。
"""

import json
import logging
import math
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# パス定義
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_PATH = BASE_DIR / "public" / "data" / "daily_risk_score.json"
OUTPUT_PATH = BASE_DIR / "public" / "data" / "kodoku_reports.json"

# ---------------------------------------------------------------------------
# チョークポイント定義 (フロントエンド src/constants/chokePoints.js と同期)
# coordinates: [longitude, latitude]
# ---------------------------------------------------------------------------
CHOKE_POINTS = [
    {"id": "hormuz", "name": "Strait of Hormuz", "coordinates": [56.48, 26.56], "type": "energy", "riskLevel": "high"},
    {"id": "malacca", "name": "Strait of Malacca", "coordinates": [100.0, 4.0], "type": "trade", "riskLevel": "medium"},
    {"id": "suez", "name": "Suez Canal", "coordinates": [32.35, 30.60], "type": "trade", "riskLevel": "high"},
    {"id": "bab_el_mandeb", "name": "Bab-el-Mandeb", "coordinates": [43.32, 12.58], "type": "energy", "riskLevel": "high"},
    {"id": "panama", "name": "Panama Canal", "coordinates": [-79.91, 9.08], "type": "trade", "riskLevel": "low"},
    {"id": "taiwan", "name": "Taiwan Strait", "coordinates": [119.5, 24.5], "type": "trade", "riskLevel": "high"},
    {"id": "bosporus", "name": "Bosporus Strait", "coordinates": [29.07, 41.02], "type": "trade", "riskLevel": "medium"},
    {"id": "cape_of_good_hope", "name": "Cape of Good Hope", "coordinates": [18.47, -34.35], "type": "trade", "riskLevel": "low"},
]

CHOKE_POINT_MAP = {cp["id"]: cp for cp in CHOKE_POINTS}

# ---------------------------------------------------------------------------
# 海運ルート定義
# 各ルートは通過するチョークポイントIDのリストを持つ
# ---------------------------------------------------------------------------
ROUTES = [
    {
        "id": "middle_east_to_japan",
        "name": "Energy Route (Middle East - Japan)",
        "chokepoints": ["hormuz", "malacca", "taiwan"],
    },
    {
        "id": "middle_east_to_europe",
        "name": "Energy Route (Middle East - Europe)",
        "chokepoints": ["hormuz", "bab_el_mandeb", "suez"],
    },
    {
        "id": "asia_to_europe_suez",
        "name": "Trade Route (Asia - Europe via Suez)",
        "chokepoints": ["malacca", "bab_el_mandeb", "suez"],
    },
    {
        "id": "asia_to_europe_cape",
        "name": "Trade Route (Asia - Europe via Cape)",
        "chokepoints": ["malacca", "cape_of_good_hope"],
    },
    {
        "id": "americas_to_asia",
        "name": "Trade Route (Americas - Asia)",
        "chokepoints": ["panama", "taiwan"],
    },
    {
        "id": "black_sea_to_mediterranean",
        "name": "Trade Route (Black Sea - Mediterranean)",
        "chokepoints": ["bosporus", "suez"],
    },
]

# ---------------------------------------------------------------------------
# 国の中心座標 (ISO3 → [longitude, latitude])
# daily_risk_score.json に含まれうるすべてのターゲット国の概算座標
# ---------------------------------------------------------------------------
COUNTRY_CENTROIDS = {
    # アフリカ
    "EGY": [30.80, 26.82], "ZAF": [25.08, -29.00], "NGA": [8.68, 9.08],
    "KEN": [37.91, 0.02], "ETH": [40.49, 9.15], "SDN": [30.22, 12.86],
    "COD": [21.76, -4.04], "SOM": [46.20, 5.15], "LBY": [17.23, 26.34],
    "MLI": [-3.99, 17.57], "BFA": [-1.56, 12.24], "NER": [8.08, 17.61],
    "TCD": [18.73, 15.45], "MOZ": [35.53, -18.67], "CAF": [20.94, 6.61],
    "CMR": [12.35, 7.37], "BDI": [29.92, -3.37], "SSD": [31.31, 6.88],
    "ZWE": [29.15, -19.02], "AGO": [17.87, -11.20],
    # 中東
    "SAU": [45.08, 23.89], "IRN": [53.69, 32.43], "IRQ": [43.68, 33.22],
    "ISR": [34.85, 31.05], "JOR": [36.24, 30.59], "LBN": [35.86, 33.87],
    "SYR": [38.99, 34.80], "YEM": [48.52, 15.55], "ARE": [53.85, 23.42],
    "QAT": [51.18, 25.35], "KWT": [47.48, 29.31], "OMN": [55.92, 21.47],
    "BHR": [50.56, 26.07], "TUR": [35.24, 38.96], "PSE": [35.23, 31.95],
    # 東南アジア+
    "IDN": [113.92, -0.79], "MYS": [101.98, 4.21], "PHL": [121.77, 12.88],
    "SGP": [103.82, 1.35], "THA": [100.99, 15.87], "VNM": [108.28, 14.06],
    "KHM": [104.99, 12.57], "LAO": [102.50, 19.86], "MMR": [96.68, 21.91],
    "BRN": [114.73, 4.54], "TLS": [125.73, -8.87], "TWN": [120.96, 23.70],
    # 南アメリカ
    "BRA": [-51.93, -14.24], "ARG": [-63.62, -38.42], "COL": [-74.30, 4.57],
    "PER": [-75.02, -9.19], "VEN": [-66.59, 6.42], "CHL": [-71.54, -35.68],
    "ECU": [-78.18, -1.83], "BOL": [-63.59, -16.29], "PRY": [-58.44, -23.44],
    "URY": [-55.77, -32.52], "GUY": [-58.93, 4.86], "SUR": [-56.03, 3.92],
}

# ---------------------------------------------------------------------------
# 距離計算パラメータ
# ---------------------------------------------------------------------------
INFLUENCE_RADIUS_KM = 1500  # チョークポイントへの影響半径 (km)
NORMALIZATION_DIVISOR = 30.0  # 危機スコアを確率に正規化するための除数


def haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """2点間の大圏距離を km で返す (Haversine の公式)。"""
    R = 6371.0  # 地球の半径 (km)
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def load_risk_data(path: Path) -> dict:
    """daily_risk_score.json を読み込む。"""
    logger.info("Loading risk data from %s", path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info("Loaded risk data for %d countries", len(data))
    return data


def compute_chokepoint_disruption(risk_data: dict) -> dict:
    """
    各チョークポイントの封鎖確率 (Disruption Risk %) を算出する。

    Gravity Model:
      各国のリスクスコアを、チョークポイントとの距離で減衰させて合算する。
      距離減衰 = max(0, (INFLUENCE_RADIUS_KM - distance_km) / INFLUENCE_RADIUS_KM)
      危機スコア = Σ (risk_score × 距離減衰)
      封鎖確率 = clip(危機スコア / NORMALIZATION_DIVISOR × 100, 0, 100)
    """
    results = {}

    for cp in CHOKE_POINTS:
        cp_lon, cp_lat = cp["coordinates"]
        crisis_score = 0.0

        for iso3, country_info in risk_data.items():
            centroid = COUNTRY_CENTROIDS.get(iso3)
            if centroid is None:
                continue

            c_lon, c_lat = centroid
            dist_km = haversine_km(cp_lon, cp_lat, c_lon, c_lat)

            if dist_km < INFLUENCE_RADIUS_KM:
                decay = (INFLUENCE_RADIUS_KM - dist_km) / INFLUENCE_RADIUS_KM
                crisis_score += country_info["risk_score"] * decay

        disruption_pct = min(max(crisis_score / NORMALIZATION_DIVISOR * 100, 0.0), 100.0)
        disruption_pct = round(disruption_pct, 1)

        results[cp["id"]] = {
            "name": cp["name"],
            "disruption_risk": disruption_pct,
            "crisis_score_raw": round(crisis_score, 4),
        }
        logger.info(
            "  %s: crisis_score=%.4f  disruption_risk=%.1f%%",
            cp["name"], crisis_score, disruption_pct,
        )

    return results


def compute_route_survival(chokepoint_risks: dict) -> list:
    """
    各ルートの生存確率を算出する。

    ボトルネック理論:
      Survival Rate = 100 - max(封鎖確率 of チョークポイント in route)
    """
    route_results = []

    for route in ROUTES:
        cp_risks = []
        for cp_id in route["chokepoints"]:
            cp_data = chokepoint_risks.get(cp_id)
            if cp_data:
                cp_risks.append((cp_id, cp_data["name"], cp_data["disruption_risk"]))

        if not cp_risks:
            continue

        # 最もリスクの高いチョークポイント (Critical Node)
        critical = max(cp_risks, key=lambda x: x[2])
        critical_id, critical_name, max_disruption = critical

        survival_rate = round(100.0 - max_disruption, 1)

        insight = generate_insight(route["name"], survival_rate, critical_name, max_disruption)

        route_results.append({
            "id": route["id"],
            "name": route["name"],
            "chokepoints": [
                {"id": cid, "name": cname, "disruption_risk": crisk}
                for cid, cname, crisk in cp_risks
            ],
            "survival_rate": survival_rate,
            "critical_node": critical_name,
            "max_disruption_risk": max_disruption,
            "insight": insight,
        })

        logger.info(
            "  Route '%s': survival=%.1f%%  critical=%s (%.1f%%)",
            route["name"], survival_rate, critical_name, max_disruption,
        )

    return route_results


def generate_insight(route_name: str, survival_rate: float, critical_node: str, disruption: float) -> str:
    """生存確率と Critical Node を元にインサイトテキストを生成する。"""
    if survival_rate >= 90:
        severity = "remains stable"
        recommendation = "No immediate rerouting action required."
    elif survival_rate >= 70:
        severity = "is under moderate pressure"
        recommendation = "Monitoring is advised; contingency routes should be reviewed."
    elif survival_rate >= 50:
        severity = f"dropped to {survival_rate}%"
        recommendation = (
            f"Alternative routing (e.g., avoiding {critical_node}) is highly recommended."
        )
    else:
        severity = f"critically declined to {survival_rate}%"
        recommendation = (
            f"Immediate rerouting away from {critical_node} is strongly urged. "
            "Supply-chain contingency plans should be activated."
        )

    return (
        f"Survival rate for {route_name} {severity}. "
        f"Critical bottleneck at {critical_node} with {disruption}% disruption risk "
        f"due to intense conflicts within {INFLUENCE_RADIUS_KM}km. "
        f"{recommendation}"
    )


def save_report(route_results: list, chokepoint_risks: dict, path: Path) -> None:
    """計算結果を JSON として保存する。"""
    report = {
        "chokepoints": chokepoint_risks,
        "routes": route_results,
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    logger.info("Report saved to %s", path)


def main() -> None:
    logger.info("=== Project KODOKU Engine Start ===")

    risk_data = load_risk_data(INPUT_PATH)
    chokepoint_risks = compute_chokepoint_disruption(risk_data)
    route_results = compute_route_survival(chokepoint_risks)
    save_report(route_results, chokepoint_risks, OUTPUT_PATH)

    logger.info("=== Project KODOKU Engine Complete ===")


if __name__ == "__main__":
    main()
