#!/usr/bin/env python3
"""CCASS Sentinel — Daily Backfill

Run a repair pass for a specific date and only fetch stocks that are missing
full data (metrics and/or holders) after a prior daily run.

Usage:
    python scripts/daily_backfill.py --date 2025/06/03
    python scripts/daily_backfill.py --date 2025/06/03 --batch-write 50 --stock-retries 4 --timeout-rounds 2
"""

import argparse
import json
import os
import sys
import time
import re
import random
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import threading

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

REPO_ROOT = Path(__file__).resolve().parent
DATA_DIR = REPO_ROOT.parent / "data"
WATCHLIST_FILE = DATA_DIR / "watchlist.json"
TIMESERIES_FILE = DATA_DIR / "ccass_timeseries.json"
HOLDERS_DIR = DATA_DIR / "holders"
HKEX_URL = "https://www3.hkexnews.hk/sdw/search/searchsdw.aspx"
PAT = re.compile(
    r'<div class="mobile-list-body">([ABC]\d{5})</div>\s*</td>\s*'
    r'<td class="col-participant-name">\s*<div[^>]*>.*?</div>\s*'
    r'<div class="mobile-list-body">(.*?)</div>\s*</td>\s*'
    r'<td class="col-address">.*?</td>\s*'
    r'<td class="col-shareholding text-right">\s*<div[^>]*>.*?</div>\s*'
    r'<div class="mobile-list-body">([\d,]+)</div>', re.DOTALL)
WORKERS = 3
JITTER = (0.8, 2.0)
DEFAULT_BATCH_WRITE = 100

_tls = threading.local()

def get_session():
    if not hasattr(_tls, "session"):
        _tls.session = requests.Session()
        _tls.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
    return _tls.session


def normalize_date(date_input):
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_input.strip(), fmt).strftime("%Y/%m/%d")
        except ValueError:
            continue
    raise ValueError(f"Invalid date format: {date_input}. Use YYYY/MM/DD or YYYY-MM-DD.")


def get_viewstate():
    s = get_session()
    r = s.get(HKEX_URL, timeout=(5, 15))
    r.raise_for_status()
    vs = re.search(r'id="__VIEWSTATE"\s+value="([^"]*)"', r.text)
    vsg = re.search(r'id="__VIEWSTATEGENERATOR"\s+value="([^"]*)"', r.text)
    ev = re.search(r'id="__EVENTVALIDATION"\s+value="([^"]*)"', r.text)
    if not all([vs, vsg]):
        return None
    return {"vs": vs.group(1), "vsg": vsg.group(1), "ev": ev.group(1) if ev else ""}


def scrape_stock(code, date_str, viewstate, retries=3):
    s = get_session()
    time.sleep(random.uniform(*JITTER))

    data = {
        "__EVENTTARGET": "btnSearch",
        "__EVENTARGUMENT": "",
        "__VIEWSTATE": viewstate["vs"],
        "__VIEWSTATEGENERATOR": viewstate["vsg"],
        "today": date_str.replace("/", ""),
        "sortBy": "shareholding",
        "sortDirection": "desc",
        "originalShareholdingDate": "",
        "alertMsg": "",
        "txtShareholdingDate": date_str,
        "txtStockCode": code,
        "txtStockName": "",
        "txtParticipantID": "",
        "txtParticipantName": "",
        "txtSelPartID": "",
    }
    if viewstate.get("ev"):
        data["__EVENTVALIDATION"] = viewstate["ev"]

    attempt = 0
    while attempt <= retries:
        attempt += 1
        try:
            r = s.post(HKEX_URL, data=data, timeout=(3.05, 20))
            if r.status_code != 200:
                return {"status": "ERROR", "holders": None, "message": f"HTTP {r.status_code}"}
            if len(r.text) < 15000:
                return {"status": "NO_DATA", "holders": None}

            matches = PAT.finditer(r.text)
            holders = sorted(
                [{"pid": m.group(1), "name": m.group(2).strip(),
                  "shares": int(m.group(3).replace(",", ""))}
                 for m in matches],
                key=lambda x: x["shares"], reverse=True
            )
            if holders:
                return {"status": "SUCCESS", "holders": holders}
            return {"status": "NO_DATA", "holders": None}

        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.Timeout) as e:
            if attempt > retries:
                return {"status": "TIMEOUT", "holders": None, "message": str(e)}
            time.sleep(0.5 + random.random())
            continue
        except Exception as e:
            return {"status": "ERROR", "holders": None, "message": str(e)}


def analyze(holdings):
    if not holdings:
        return {}
    total = sum(h["shares"] for h in holdings)
    if total == 0:
        return {}

    a5_shares = sum(h["shares"] for h in holdings if h["pid"] == "A00005")
    adjusted_float = total - a5_shares
    is_h_share = a5_shares / total > 0.3

    adj = [h for h in holdings if h["pid"] != "A00005"]
    brokers = [h for h in adj if h["pid"].startswith("B")]

    if adjusted_float > 0:
        adj_top5 = sum(h["shares"] for h in adj[:5]) / adjusted_float * 100
        broker_top5 = sum(h["shares"] for h in brokers[:5]) / adjusted_float * 100
        adj_hhi = sum((h["shares"] / adjusted_float * 100) ** 2 for h in adj)
    else:
        adj_top5 = broker_top5 = adj_hhi = 0

    top_broker = brokers[0] if brokers else None
    futu = next((h["shares"] for h in holdings if h["pid"] == "B01955"), 0)

    return {
        "date": None,
        "total_shares": total,
        "a00005_pct": round(a5_shares / total * 100, 2),
        "is_h_share": is_h_share,
        "adjusted_float": adjusted_float,
        "adj_top5_pct": round(adj_top5, 2),
        "adj_hhi": round(adj_hhi, 1),
        "broker_top5_pct": round(broker_top5, 2),
        "top_broker_id": top_broker["pid"] if top_broker else "",
        "top_broker_name": top_broker["name"][:40] if top_broker else "",
        "top_broker_pct": round(top_broker["shares"] / adjusted_float * 100, 2) if top_broker and adjusted_float > 0 else 0,
        "futu_pct": round(futu / adjusted_float * 100, 2) if adjusted_float > 0 else 0,
        "participant_count": len(holdings),
        "holders": [{"pid": h["pid"], "name": h["name"], "shares": h["shares"]} for h in holdings],
    }


def atomic_write(path, data):
    tmp = str(path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, str(path))


def load_json(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def merge_holders(existing, update):
    for code, holders in update.items():
        if holders:
            existing[code] = holders
        else:
            if code not in existing:
                existing[code] = holders
    return existing


def main():
    parser = argparse.ArgumentParser(description="CCASS Sentinel Daily Backfill")
    parser.add_argument("--date", help="CCASS date (YYYY/MM/DD). Default: yesterday")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing files")
    parser.add_argument(
        "--batch-write",
        type=int,
        default=DEFAULT_BATCH_WRITE,
        help="Write intermediate holders file every N completed stocks. Default: 100",
    )
    parser.add_argument(
        "--stock-retries",
        type=int,
        default=3,
        help="Retry each stock on network timeout this many times. Default: 3",
    )
    parser.add_argument(
        "--timeout-rounds",
        type=int,
        default=1,
        help="Retry timed-out stocks after the first pass. Default: 1",
    )
    args = parser.parse_args()

    if args.date:
        try:
            date_str = normalize_date(args.date)
        except ValueError as exc:
            print(f"  ❌ {exc}")
            sys.exit(1)
    else:
        yesterday = datetime.now() - timedelta(days=1)
        date_str = yesterday.strftime("%Y/%m/%d")

    date_key = date_str.replace("/", "-")
    print(f"🛠️  CCASS Sentinel Backfill — {date_str}")
    batch_write = max(1, args.batch_write)
    stock_retries = max(0, args.stock_retries)
    timeout_rounds = max(0, args.timeout_rounds)

    if not WATCHLIST_FILE.exists():
        print(f"  ❌ No watchlist at {WATCHLIST_FILE}")
        sys.exit(1)

    watchlist = load_json(WATCHLIST_FILE)
    codes = [w["code"] for w in watchlist]
    print(f"  Watchlist: {len(codes)} stocks")

    ts = load_json(TIMESERIES_FILE)
    holders_file = HOLDERS_DIR / f"{date_key}.json"
    existing_holders = load_json(holders_file)

    targets = []
    for code in codes:
        has_metrics = code in ts and date_key in ts[code]
        has_holders = code in existing_holders and existing_holders[code]
        if not has_metrics or not has_holders:
            targets.append(code)
    if not targets:
        print("  ✅ No missing stocks detected for this date. All complete.")
        return

    print(f"  Targets: {len(targets)} stocks need repair")
    vs = get_viewstate()
    if not vs:
        print("  ❌ Failed to get ViewState")
        sys.exit(1)

    daily_holders = {}
    known_no_data = set()
    pending_timeouts = []
    errors = []
    collected = 0
    processed = 0
    holders_lock = threading.Lock()

    def process_result(code, result, is_final_round=False):
        nonlocal collected, processed
        processed += 1
        status = result.get("status") if isinstance(result, dict) else None

        if status == "SUCCESS":
            holders = result["holders"]
            metrics = analyze(holders)
            metrics["date"] = date_key
            if code not in ts:
                ts[code] = {}
            ts[code][date_key] = metrics
            with holders_lock:
                daily_holders[code] = holders
            collected += 1
            return

        if status == "NO_DATA":
            known_no_data.add(code)
            with holders_lock:
                if code not in existing_holders:
                    existing_holders[code] = []
            return

        if status == "TIMEOUT" and not is_final_round:
            pending_timeouts.append(code)
            return

        if status == "TIMEOUT":
            errors.append((code, result.get("message", "timeout")))
            return

        errors.append((code, result.get("message", "unknown")))

    def run_round(codes_to_fetch, is_final_round=False):
        if not codes_to_fetch:
            return []
        pending_timeouts.clear()
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(scrape_stock, code, date_str, vs, stock_retries): code for code in codes_to_fetch}
            done_set = set()
            while futures:
                done, _ = wait(futures.keys() - done_set, return_when=FIRST_COMPLETED)
                for f in done:
                    done_set.add(f)
                    code = futures[f]
                    result = f.result()
                    process_result(code, result, is_final_round=is_final_round)
                    if processed % 10 == 0:
                        print(f"  Progress: {processed} processed, {collected} collected, {len(pending_timeouts)} pending timeouts, {len(errors)} errors")
                    if collected > 0 and collected % batch_write == 0:
                        try:
                            HOLDERS_DIR.mkdir(parents=True, exist_ok=True)
                            merged = merge_holders(existing_holders, daily_holders)
                            if any(merged.values()):
                                atomic_write(holders_file, merged)
                                print(f"  💾 Interim holders saved ({len(merged)} entries)")
                        except Exception as e:
                            print(f"  ⚠️ Failed interim write: {e}")
                if done_set == set(futures.keys()):
                    break
        return list(pending_timeouts)

    pending_timeouts = run_round(targets)
    for round_index in range(timeout_rounds):
        if not pending_timeouts:
            break
        print(f"  🔁 Retrying {len(pending_timeouts)} timed-out stocks (round {round_index + 1}/{timeout_rounds})...")
        pending_timeouts = run_round(pending_timeouts, is_final_round=(round_index == timeout_rounds - 1))

    merged_holders = merge_holders(existing_holders, daily_holders)
    if not args.dry_run:
        HOLDERS_DIR.mkdir(parents=True, exist_ok=True)
        if any(merged_holders.values()):
            atomic_write(holders_file, merged_holders)
        if ts:
            for code in ts:
                for dk in ts[code]:
                    ts[code][dk].pop("holders", None)
            atomic_write(TIMESERIES_FILE, ts)

    missing_after = []
    for code in codes:
        has_metrics = code in ts and date_key in ts[code]
        has_holders = code in merged_holders and merged_holders[code]
        if not has_metrics or not has_holders:
            if code not in known_no_data:
                missing_after.append(code)

    print("\n🔎 Backfill result:")
    print(f"  Collected: {collected}")
    print(f"  Known no-data: {len(known_no_data)}")
    print(f"  Timed-out after retries: {len(pending_timeouts)}")
    print(f"  Errors: {len(errors)}")
    print(f"  Still missing: {len(missing_after)}")
    if missing_after:
        print("  Missing stock codes:")
        for code in missing_after[:50]:
            print(f"    - {code}")
        if len(missing_after) > 50:
            print(f"    ... and {len(missing_after) - 50} more")
        print("\n  ⚠️ Some stocks still need manual follow-up or a network retry.")
        sys.exit(1)
    else:
        print("  ✅ All stocks either collected or recorded as no-data.")
        sys.exit(0)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
