#!/usr/bin/env python3

from __future__ import annotations

import csv
import math
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import os

import requests

try:
    from zoneinfo import ZoneInfo
except ImportError:
    raise SystemExit("Python 3.9+ required (zoneinfo).")

BUDAPEST_TZ = ZoneInfo("Europe/Budapest")

ANU_API_URL = "https://api.quantumnumbers.anu.edu.au"
ANU_API_KEY_ENV = "ANU_QRNG_API_KEY"

COINBASE_SPOT_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"

REPO_ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = REPO_ROOT / "data" / "data.csv"
HEADER = ["t", "q", "v", "qV", "qP", "aV", "aP"]


def _round(x: Optional[float], ndigits: int) -> str:
    if x is None:
        return ""
    if not math.isfinite(x):
        return ""
    return f"{round(x, ndigits):.{ndigits}f}"


def _get_anu_api_key() -> str:
    key = os.environ.get(ANU_API_KEY_ENV)
    if not key or not key.strip():
        raise SystemExit(
            f"Missing required environment variable {ANU_API_KEY_ENV}. "
            "Set it from your GitHub Actions repo secret."
        )
    return key.strip()


def _truncate_body(s: str, max_len: int = 400) -> str:
    s = (s or "").strip()
    s = s.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n")
    if len(s) > max_len:
        return s[:max_len] + "…"
    return s


def fetch_anu_uint16() -> int:
    key = _get_anu_api_key()
    headers = {"x-api-key": key}
    params = {"length": 1, "type": "uint16"}

    try:
        r = requests.get(ANU_API_URL, headers=headers, params=params, timeout=20)
    except requests.RequestException as e:
        raise SystemExit(f"ANU request failed: {e}") from e

    if r.status_code != 200:
        body = _truncate_body(r.text)
        raise SystemExit(
            "ANU API request rejected. "
            f"status={r.status_code} url={r.url} key_len={len(key)} body={body}"
        )

    try:
        j = r.json()
    except ValueError as e:
        body = _truncate_body(r.text)
        raise SystemExit(
            f"ANU API returned non-JSON response. status={r.status_code} url={r.url} body={body}"
        ) from e

    if "data" not in j or not isinstance(j["data"], list) or len(j["data"]) != 1:
        raise ValueError(f"Unexpected ANU payload shape: {j}")

    n = int(j["data"][0])
    if not (0 <= n <= 65535):
        raise ValueError(f"ANU uint16 out of range: {n}")
    return n


def map_uint16_to_q(n: int) -> float:
    return -1.0 + 2.0 * (n / 65535.0)


def fetch_btc_usd_spot() -> float:
    r = requests.get(COINBASE_SPOT_URL, timeout=20)
    r.raise_for_status()
    j = r.json()
    amount = j.get("data", {}).get("amount")
    if amount is None:
        raise ValueError(f"Unexpected Coinbase payload shape: {j}")
    return float(amount)


def read_csv() -> List[Dict[str, str]]:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Missing CSV at {CSV_PATH} (expected committed file).")

    with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != HEADER:
            raise ValueError(f"CSV header mismatch. Expected {HEADER}, got {reader.fieldnames}")
        return list(reader)


def write_csv(rows: List[Dict[str, str]]) -> None:
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in HEADER})


def has_row_for_date(rows: List[Dict[str, str]], yyyy_mm_dd: str) -> bool:
    for r in rows:
        t = (r.get("t") or "").strip()
        if len(t) >= 10 and t[:10] == yyyy_mm_dd:
            return True
    return False


def is_empty(s: Optional[str]) -> bool:
    return s is None or str(s).strip() == ""


def main() -> int:
    now_local = datetime.now(BUDAPEST_TZ)
    today = now_local.date().isoformat()

    if not (now_local.hour >= 20 and now_local.hour < 22):
        print(f"[SKIP] Outside local window. now={now_local.isoformat(timespec='seconds')}")
        return 0

    rows = read_csv()

    if has_row_for_date(rows, today):
        print(f"[SKIP] Row already exists for {today}")
        return 0

    n = fetch_anu_uint16()
    q = map_uint16_to_q(n)
    v = fetch_btc_usd_spot()

    qV = v * (1.0 + q)
    qP = q

    if rows:
        prev = rows[-1]
        if is_empty(prev.get("aV")) and is_empty(prev.get("aP")) and not is_empty(prev.get("qV")):
            try:
                prev_qV = float(prev["qV"])
            except Exception:
                prev_qV = None

            if prev_qV is not None and math.isfinite(prev_qV):
                aV = abs(v - prev_qV)
                aP = (aV / prev_qV) if prev_qV != 0 else None

                prev["aV"] = _round(aV, 2)
                prev["aP"] = _round(aP, 6) if aP is not None else ""

    t = now_local.isoformat(timespec="seconds")
    new_row = {
        "t": t,
        "q": _round(q, 6),
        "v": _round(v, 2),
        "qV": _round(qV, 2),
        "qP": _round(qP, 6),
        "aV": "",
        "aP": "",
    }
    rows.append(new_row)

    write_csv(rows)
    print(f"[OK] appended {today} t={t} q={new_row['q']} v={new_row['v']} qV={new_row['qV']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
