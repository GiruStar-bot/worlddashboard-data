"""
fetch_gdelt.py
GDELT 2.0 Events Database から最新データを取得し、特定地域の紛争リスクを集計して
public/data/daily_risk_score.json に保存するスクリプト。
"""

import io
import json
import logging
import zipfile
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GDELT CSV のカラム定義（全 58 列）
# ---------------------------------------------------------------------------
GDELT_COLUMNS = [
    "GLOBALEVENTID", "SQLDATE", "MonthYear", "Year", "FractionDate",
    "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode",
    "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code",
    "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
    "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode",
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code",
    "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
    "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode",
    "QuadClass", "GoldsteinScale", "NumMentions", "NumSources",
    "NumArticles", "AvgTone", "Actor1Geo_Type", "Actor1Geo_FullName",
    "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
    "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat",
    "Actor2Geo_Long", "Actor2Geo_FeatureID", "ActionGeo_Type",
    "ActionGeo_FullName", "ActionGeo_CountryCode", "ActionGeo_ADM1Code",
    "ActionGeo_ADM2Code", "ActionGeo_Lat", "ActionGeo_Long",
    "ActionGeo_FeatureID", "DATEADDED", "SOURCEURL",
]

# ---------------------------------------------------------------------------
# FIPS 10-4 → ISO 3166-1 alpha-3 変換辞書
# GDELTの Actor1Geo_CountryCode は FIPS 10-4 形式
# ---------------------------------------------------------------------------
FIPS_TO_ISO3 = {
    "US": "USA", "CH": "CHN", "JA": "JPN", "RS": "RUS", "GM": "DEU",
    "FR": "FRA", "UK": "GBR", "IT": "ITA", "SP": "ESP", "PO": "PRT",
    "AU": "AUS", "CA": "CAN", "MX": "MEX", "BR": "BRA", "AR": "ARG",
    "IN": "IND", "PK": "PAK", "AF": "AFG", "IZ": "IRQ", "IR": "IRN",
    "SY": "SYR", "IS": "ISR", "JO": "JOR", "LE": "LBN", "SA": "SAU",
    "AE": "ARE", "QA": "QAT", "KU": "KWT", "BA": "BHR", "YM": "YEM",
    "TU": "TUR", "EG": "EGY", "LY": "LBY", "TS": "TUN", "MO": "MAR",
    "AG": "DZA", "SU": "SDN", "ET": "ETH", "KE": "KEN", "SO": "SOM",
    "NI": "NGA", "GH": "GHA", "SG": "SEN", "ML": "MLI", "IV": "CIV",
    "ZI": "ZWE", "ZA": "ZAF", "AO": "AGO", "MZ": "MOZ", "TZ": "TZA",
    "UG": "UGA", "RW": "RWA", "CG": "COD", "CF": "CAF", "CM": "CMR",
    "GA": "GAB", "SE": "SWE", "NO": "NOR", "FI": "FIN", "DA": "DNK",
    "NL": "NLD", "BE": "BEL", "SW": "CHE", "AU": "AUT", "PL": "POL",
    "EZ": "CZE", "LO": "SVK", "HU": "HUN", "RO": "ROU", "BU": "BGR",
    "GR": "GRC", "AL": "ALB", "HR": "HRV", "BO": "BIH", "SR": "SRB",
    "MK": "MKD", "SI": "SVN", "LH": "LTU", "LG": "LVA", "EN": "EST",
    "MD": "MDA", "UP": "UKR", "BY": "BLR", "GG": "GEO", "AM": "ARM",
    "AJ": "AZE", "KZ": "KAZ", "UZ": "UZB", "TM": "TKM", "KG": "KGZ",
    "TI": "TJK", "MN": "MNG", "KS": "KOR", "KN": "PRK", "TW": "TWN",
    "VM": "VNM", "TH": "THA", "MY": "MYS", "SN": "SGP", "PH": "PHL",
    "ID": "IDN", "BM": "MMR", "CB": "KHM", "LA": "LAO", "NP": "NPL",
    "BG": "BGD", "CE": "LKA", "MV": "MDV", "BT": "BTN",
    "NZ": "NZL", "FJ": "FJI", "PP": "PNG",
    "CU": "CUB", "CO": "COL", "VE": "VEN", "PE": "PER", "CI": "CHL",
    "BO": "BOL", "EC": "ECU", "UY": "URY", "PY": "PRY",
    "GT": "GTM", "HO": "HND", "ES": "SLV", "NU": "NIC", "CS": "CRI",
    "PM": "PAN", "BH": "BLZ", "JM": "JAM", "TD": "TTO", "DR": "DOM",
    "HA": "HTI", "CJ": "CYM",
    "IC": "ISL", "LU": "LUX", "MT": "MLT", "CY": "CYP",
    "MU": "MUS", "MP": "COM", "SC": "SYC", "CV": "CPV",
}

# 重複キーの上書きを防ぐため Austria (AU) は AUT に、Australia (AS) は AUS に整理
FIPS_TO_ISO3["AS"] = "AUS"
FIPS_TO_ISO3["AU"] = "AUT"

# GDELT FIPS に BFA/NER/TCD/BDI/SSD/GUY/SUR/OMN/BRN/TLS/PSE 用のマッピングを追加
FIPS_TO_ISO3.update({
    "UV": "BFA",  # Burkina Faso
    "NG": "NER",  # Niger
    "CD": "TCD",  # Chad
    "BY": "BDI",  # Burundi
    "OD": "SSD",  # South Sudan
    "GY": "GUY",  # Guyana
    "NS": "SUR",  # Suriname
    "MU": "OMN",  # Oman
    "BX": "BRN",  # Brunei
    "TT": "TLS",  # Timor-Leste
    "GZ": "PSE",  # Palestinian Territories (Gaza)
    "WE": "PSE",  # Palestinian Territories (West Bank)
})

# ---------------------------------------------------------------------------
# ターゲット地域 (ISO 3166-1 alpha-3)
# ---------------------------------------------------------------------------
TARGET_ISO3 = {
    # アフリカ大陸
    "EGY", "ZAF", "NGA", "KEN", "ETH", "SDN", "COD", "SOM", "LBY", "MLI",
    "BFA", "NER", "TCD", "MOZ", "CAF", "CMR", "BDI", "SSD", "ZWE", "AGO",
    # 中東
    "SAU", "IRN", "IRQ", "ISR", "JOR", "LBN", "SYR", "YEM", "ARE", "QAT",
    "KWT", "OMN", "BHR", "TUR", "PSE",
    # 東南アジア+
    "IDN", "MYS", "PHL", "SGP", "THA", "VNM", "KHM", "LAO", "MMR", "BRN",
    "TLS", "TWN",
    # 南アメリカ
    "BRA", "ARG", "COL", "PER", "VEN", "CHL", "ECU", "BOL", "PRY", "URY",
    "GUY", "SUR",
}

# ---------------------------------------------------------------------------
# 出力先
# ---------------------------------------------------------------------------
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "public" / "data" / "daily_risk_score.json"

LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"


def fetch_latest_export_url() -> str:
    """lastupdate.txt から最新の export.CSV.zip の URL を取得する。"""
    logger.info("Fetching lastupdate.txt ...")
    resp = requests.get(LASTUPDATE_URL, timeout=30)
    resp.raise_for_status()
    for line in resp.text.splitlines():
        parts = line.strip().split()
        if len(parts) >= 3 and parts[2].endswith("export.CSV.zip"):
            url = parts[2]
            logger.info("Found export URL: %s", url)
            return url
    raise ValueError("export.CSV.zip URL not found in lastupdate.txt")


def download_and_parse(url: str) -> pd.DataFrame:
    """ZIP をメモリ上でダウンロード・解凍し DataFrame として返す。"""
    logger.info("Downloading %s ...", url)
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_name = next(n for n in zf.namelist() if n.endswith(".CSV"))
        logger.info("Extracting %s ...", csv_name)
        with zf.open(csv_name) as f:
            df = pd.read_csv(
                f,
                sep="\t",
                header=None,
                names=GDELT_COLUMNS,
                low_memory=False,
            )

    logger.info("Loaded %d rows", len(df))
    return df


def process(df: pd.DataFrame) -> dict:
    """DataFrame を国別に集計して紛争リスクスコア辞書を返す。"""
    # 必要カラムのみ抽出
    cols = ["Actor1Geo_CountryCode", "QuadClass", "EventRootCode", "GoldsteinScale", "SOURCEURL"]
    df = df[cols].copy()

    # 数値型を強制
    df["QuadClass"] = pd.to_numeric(df["QuadClass"], errors="coerce")
    df["EventRootCode"] = pd.to_numeric(df["EventRootCode"], errors="coerce")
    df["GoldsteinScale"] = pd.to_numeric(df["GoldsteinScale"], errors="coerce")

    # 物理的な紛争（QuadClass=4）または抗議デモ・暴動（EventRootCode=14）のみ残す
    df = df[(df["QuadClass"] == 4) | (df["EventRootCode"] == 14)]

    # 国コードが空の行を除外
    df = df[df["Actor1Geo_CountryCode"].notna() & (df["Actor1Geo_CountryCode"].str.strip() != "")]

    # FIPS → ISO3 変換（辞書にないコードは除外）
    df["iso3"] = df["Actor1Geo_CountryCode"].str.strip().map(FIPS_TO_ISO3)
    df = df[df["iso3"].notna()]

    # ターゲット地域のみに絞り込む
    df = df[df["iso3"].isin(TARGET_ISO3)]

    # BaseScore = abs(GoldsteinScale)
    df["BaseScore"] = df["GoldsteinScale"].abs()

    # 国ごとに最もBaseScoreが高い（深刻な）記事のURLを取得
    df_valid = df.dropna(subset=["BaseScore", "SOURCEURL"])
    top_news = (
        df_valid.sort_values("BaseScore", ascending=False)
        .groupby("iso3")["SOURCEURL"]
        .first()
    )

    # 集計: Risk Score = sum(BaseScore) / 10
    agg = df.groupby("iso3").agg(
        risk_score=("BaseScore", lambda x: x.sum() / 10),
        count=("BaseScore", "size"),
    )

    agg["top_news"] = top_news
    agg["top_news"] = agg["top_news"].fillna("")

    # 足切り: Risk Score < 2.0 の国を除外
    agg = agg[agg["risk_score"] >= 2.0]

    result = {}
    for iso3, row in agg.iterrows():
        result[iso3] = {
            "risk_score": round(float(row["risk_score"]), 4),
            "count": int(row["count"]),
            "top_news": str(row["top_news"]),
        }

    logger.info("Aggregated %d countries", len(result))
    return result


def save(data: dict, path: Path) -> None:
    """JSON を指定パスに保存する。ディレクトリが存在しない場合は作成する。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info("Saved to %s", path)


def main() -> None:
    url = fetch_latest_export_url()
    df = download_and_parse(url)
    data = process(df)
    save(data, OUTPUT_PATH)
    logger.info("Done.")


if __name__ == "__main__":
    main()
