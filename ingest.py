import argparse
from datetime import date, datetime
from io import BytesIO, StringIO
import re
from typing import Iterable, Optional
from urllib.parse import urljoin

import duckdb
import pandas as pd
import requests
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator

HOLDINGS_URL = "https://amplifyetfs.com/yyy-holdings/"
HOLDINGS_FEED_URL = (
    "https://amplifyetfs.com/wp-content/uploads/feeds/AmplifyWeb.40XL.XL_Holdings.csv"
)
DEFAULT_FUND_TICKER = "YYY"
DB_PATH = "holdings.duckdb"


class HoldingRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    date: date
    ticker: str = Field(min_length=1)
    name: str = Field(min_length=1)
    shares: float
    market_value: float
    weight: float

    @field_validator("ticker", "name")
    @classmethod
    def _strip_strings(cls, value: str) -> str:
        return value.strip()

    @field_validator("ticker")
    @classmethod
    def _normalize_ticker(cls, value: str) -> str:
        return value.upper()


def _normalize_column(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lower())


def _find_csv_link(html: str, base_url: str) -> Optional[str]:
    match = re.search(r'href=["\']([^"\']+\.csv[^"\']*)["\']', html, re.IGNORECASE)
    if not match:
        return None
    return urljoin(base_url, match.group(1))


def _extract_fund_ticker(html: str) -> Optional[str]:
    match = re.search(r"AmplifyFundName\\s*=\\s*['\"]([^'\"]+)['\"]", html)
    if not match:
        return None
    return match.group(1).strip()


def _pick_holdings_table(tables: Iterable[pd.DataFrame]) -> pd.DataFrame:
    tables = list(tables)
    for table in tables:
        columns = [_normalize_column(col) for col in table.columns]
        if "ticker" in columns or "symbol" in columns or "stockticker" in columns:
            return table
    return tables[0]


def _parse_number(value) -> Optional[float]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text == "":
        return None
    text = text.replace(",", "")
    text = text.replace("$", "")
    text = text.replace("%", "")
    text = text.replace("(", "-").replace(")", "")
    try:
        return float(text)
    except ValueError:
        return None


def _coerce_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join([str(part) for part in col if str(part) != "nan"]).strip()
            for col in df.columns
        ]

    normalized = {_normalize_column(col): col for col in df.columns}

    mapping = {}
    if "ticker" in normalized:
        mapping[normalized["ticker"]] = "ticker"
    elif "symbol" in normalized:
        mapping[normalized["symbol"]] = "ticker"
    elif "stockticker" in normalized:
        mapping[normalized["stockticker"]] = "ticker"

    if "name" in normalized:
        mapping[normalized["name"]] = "name"
    elif "holding" in normalized:
        mapping[normalized["holding"]] = "name"
    elif "security" in normalized:
        mapping[normalized["security"]] = "name"
    elif "securityname" in normalized:
        mapping[normalized["securityname"]] = "name"

    if "shares" in normalized:
        mapping[normalized["shares"]] = "shares"
    elif "shs" in normalized:
        mapping[normalized["shs"]] = "shares"
    elif "sharesparvalue" in normalized:
        mapping[normalized["sharesparvalue"]] = "shares"

    if "marketvalue" in normalized:
        mapping[normalized["marketvalue"]] = "market_value"
    elif "marketvalueusd" in normalized:
        mapping[normalized["marketvalueusd"]] = "market_value"

    if "weight" in normalized:
        mapping[normalized["weight"]] = "weight"
    elif "weighting" in normalized:
        mapping[normalized["weighting"]] = "weight"
    elif "weightings" in normalized:
        mapping[normalized["weightings"]] = "weight"
    elif "percentofnav" in normalized:
        mapping[normalized["percentofnav"]] = "weight"
    elif "weightofnav" in normalized:
        mapping[normalized["weightofnav"]] = "weight"
    elif "percentofnetassets" in normalized:
        mapping[normalized["percentofnetassets"]] = "weight"
    elif "pctofnav" in normalized:
        mapping[normalized["pctofnav"]] = "weight"
    elif "percentmarketvalue" in normalized:
        mapping[normalized["percentmarketvalue"]] = "weight"
    else:
        weight_like = [
            key
            for key in normalized
            if "weight" in key and "average" not in key and "avg" not in key
        ]
        if weight_like:
            mapping[normalized[weight_like[0]]] = "weight"

    df = df.rename(columns=mapping)
    required = ["ticker", "name", "shares", "market_value"]
    missing = set(required).difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    if "weight" not in df.columns:
        df["weight"] = None

    df = df[required + ["weight"]].copy()
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df["name"] = df["name"].astype(str).str.strip()
    df = df[df["ticker"].notna() & (df["ticker"] != "")]
    df = df[df["ticker"].str.lower() != "nan"]
    df = df[df["ticker"].str.lower() != "total"]

    df["shares"] = df["shares"].map(_parse_number)
    df["market_value"] = df["market_value"].map(_parse_number)
    df["weight"] = df["weight"].map(_parse_number)

    if df["weight"].isna().any():
        total_market_value = df["market_value"].sum(skipna=True)
        if total_market_value:
            missing_mask = df["weight"].isna()
            df.loc[missing_mask, "weight"] = (
                df.loc[missing_mask, "market_value"] / total_market_value * 100
            )

    df = df.dropna(subset=["shares", "market_value", "weight"])

    return df


def _extract_as_of_date(df: pd.DataFrame) -> Optional[date]:
    if df.empty:
        return None
    normalized = {_normalize_column(col): col for col in df.columns}
    for key in ("date", "asofdate", "asof"):
        if key in normalized:
            series = df[normalized[key]].dropna()
            if series.empty:
                continue
            parsed = pd.to_datetime(series.iloc[0], errors="coerce")
            if pd.notna(parsed):
                return parsed.date()
    return None


def _fetch_holdings_feed(
    fund_ticker: str, headers: dict[str, str]
) -> Optional[pd.DataFrame]:
    try:
        response = requests.get(HOLDINGS_FEED_URL, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException:
        return None

    df = pd.read_csv(BytesIO(response.content))
    fund_ticker = fund_ticker.upper()
    if "Account" in df.columns:
        df = df[df["Account"].astype(str).str.upper() == fund_ticker]
    elif "Account Ticker" in df.columns:
        df = df[df["Account Ticker"].astype(str).str.upper() == fund_ticker]
    elif "Fund Ticker" in df.columns:
        df = df[df["Fund Ticker"].astype(str).str.upper() == fund_ticker]
    return df


def fetch_holdings(url: str) -> pd.DataFrame:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    html = None
    fund_ticker = DEFAULT_FUND_TICKER
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        html = response.text
        fund_ticker = _extract_fund_ticker(html) or DEFAULT_FUND_TICKER
    except requests.RequestException:
        html = None

    if html:
        csv_link = _find_csv_link(html, url)
        if csv_link:
            csv_response = requests.get(csv_link, headers=headers, timeout=30)
            csv_response.raise_for_status()
            return pd.read_csv(BytesIO(csv_response.content))

    feed_df = _fetch_holdings_feed(fund_ticker, headers)
    if feed_df is not None and not feed_df.empty:
        return feed_df
    if html:
        tables = pd.read_html(StringIO(html))
        if not tables:
            raise ValueError("No tables found on holdings page.")
        return _pick_holdings_table(tables)

    raise ValueError("No holdings data found.")


def validate_holdings(df: pd.DataFrame, as_of_date: Optional[date]) -> pd.DataFrame:
    if as_of_date is None:
        as_of_date = _extract_as_of_date(df) or date.today()

    df = _coerce_columns(df)
    df["date"] = as_of_date

    adapter = TypeAdapter(list[HoldingRecord])
    records = adapter.validate_python(df.to_dict(orient="records"))
    validated = pd.DataFrame([record.model_dump() for record in records])
    return validated.drop_duplicates(subset=["date", "ticker"], keep="last")


def upsert_holdings(df: pd.DataFrame, db_path: str) -> None:
    if df.empty:
        raise ValueError("No holdings rows to load.")

    holding_date = df["date"].iloc[0]
    con = duckdb.connect(db_path)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS holdings (
            date DATE,
            ticker VARCHAR,
            name VARCHAR,
            shares DOUBLE,
            market_value DOUBLE,
            weight DOUBLE
        )
        """
    )
    con.execute("DELETE FROM holdings WHERE date = ?", [holding_date])
    con.register("incoming_holdings", df)
    con.execute(
        """
        INSERT INTO holdings (date, ticker, name, shares, market_value, weight)
        SELECT date, ticker, name, shares, market_value, weight
        FROM incoming_holdings
        """
    )
    con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest YYY holdings into DuckDB.")
    parser.add_argument("--date", dest="as_of_date", help="Override as-of date (YYYY-MM-DD).")
    parser.add_argument("--db", dest="db_path", default=DB_PATH, help="DuckDB file path.")
    parser.add_argument("--url", dest="url", default=HOLDINGS_URL, help="Holdings page URL.")
    args = parser.parse_args()

    if args.as_of_date:
        as_of_date = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()
    else:
        as_of_date = None

    raw = fetch_holdings(args.url)
    validated = validate_holdings(raw, as_of_date)
    upsert_holdings(validated, args.db_path)
    holding_date = validated["date"].iloc[0]
    print(
        f"Ingested {len(validated)} holdings for {holding_date} into {args.db_path}."
    )


if __name__ == "__main__":
    main()
