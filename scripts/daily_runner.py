#!/usr/bin/env python3
"""CCASS Sentinel — Daily Runner

Scrapes today's CCASS for all watchlist stocks, appends to time-series
dataset, runs anomaly detection, outputs alerts.

Usage:
    python daily_runner.py                        # auto-detect yesterday's settlement date
    python daily_runner.py --date 2026/03/19      # specific date
    python daily_runner.py --dry-run              # scrape but don't commit
"""

import argparse, json, os, sys, time, re, math, subprocess
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import threading, random

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

# ── Config ──────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
WATCHLIST_FILE = DATA_DIR / "watchlist.json"
TIMESERIES_FILE = DATA_DIR / "ccass_timeseries.json"  # Tier 1: metrics only (~40KB/day)
HOLDERS_DIR = DATA_DIR / "holders"                     # Tier 2: full holders (~1.3MB/day, git-lfs)
ALERTS_DIR = REPO_ROOT / "alerts"

HKEX_URL = "https://www3.hkexnews.hk/sdw/search/searchsdw.aspx"
PAT = re.compile(
    r'<div class="mobile-list-body">([ABC]\d{5})</div>\s*</td>\s*'
    r'<td class="col-participant-name">\s*<div[^>]*>.*?</div>\s*'
    r'<div class="mobile-list-body">(.*?)</div>\s*</td>\s*'
    r'<td class="col-address">.*?</td>\s*'
    r'<td class="col-shareholding text-right">\s*<div[^>]*>.*?</div>\s*'
    r'<div class="mobile-list-body">([\d,]+)</div>', re.DOTALL)
WORKERS = 5  # Conservative for daily runs
JITTER = (0.8, 1.5)
BATCH_HOLDERS_WRITE = 100  # write holders file every N collected

# ── Scraping (reuse proven architecture from collector_v5) ──────────────

_tls = threading.local()

def get_session():
    if not hasattr(_tls, "session"):
        _tls.session = requests.Session()
        _tls.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
    return _tls.session

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


def normalize_date(date_input):
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_input.strip(), fmt).strftime("%Y/%m/%d")
        except ValueError:
            continue
    raise ValueError(f"Invalid date format: {date_input}. Use YYYY/MM/DD or YYYY-MM-DD.")


def scrape_stock(code, date_str, viewstate, retries=2):
    """Scrape a single stock's CCASS data. Retries on network timeouts.
    Returns a dict with status and holders."""
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
    """Option A concentration metrics."""
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
        "date": None,  # filled by caller
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


# ── Anomaly Detection ──────────────────────────────────────────────────

def detect_anomalies(code, today_data, history):
    """Compare today's snapshot against historical baseline. Returns list of alerts."""
    alerts = []
    if not today_data or not history:
        return alerts
    
    # Get most recent prior snapshot
    prior_dates = sorted(history.keys())
    if not prior_dates:
        return alerts
    prior = history[prior_dates[-1]]
    
    bt5_now = today_data.get("broker_top5_pct", 0)
    bt5_prior = prior.get("broker_top5_pct", 0)
    bt5_delta = bt5_now - bt5_prior
    
    # Alert 1: Broker concentration jumped >3pp in one day
    if bt5_delta > 3:
        alerts.append({
            "type": "BROKER_SPIKE",
            "severity": "HIGH",
            "message": f"{code}: BrkT5 jumped {bt5_delta:+.1f}pp in 1 day ({bt5_prior:.1f}%→{bt5_now:.1f}%)",
        })
    
    # Alert 2: Participant count dropped >10% in one day
    parts_now = today_data.get("participant_count", 0)
    parts_prior = prior.get("participant_count", 0)
    if parts_prior > 0 and (parts_now - parts_prior) / parts_prior < -0.10:
        alerts.append({
            "type": "PARTICIPANT_DROP",
            "severity": "MEDIUM",
            "message": f"{code}: Participant count dropped {parts_now-parts_prior} ({parts_prior}→{parts_now})",
        })
    
    # Alert 3: Cluster broker appeared or grew >5pp
    # Note: prior data from Tier 1 may not have holders. Load from Tier 2 if available.
    cluster_pids = {"B02082", "B02120", "B02165", "B01959"}
    af = today_data.get("adjusted_float", 0)
    prior_holders = prior.get("holders", [])
    prior_af = prior.get("adjusted_float", 0)
    if af > 0 and prior_holders and prior_af > 0:
        prior_map = {h["pid"]: h["shares"] / prior_af * 100 for h in prior_holders}
        for h in today_data.get("holders", []):
            if h["pid"] in cluster_pids:
                pct_now = h["shares"] / af * 100
                pct_prior = prior_map.get(h["pid"], 0)
                if pct_now - pct_prior > 5:
                    alerts.append({
                        "type": "CLUSTER_ALERT",
                        "severity": "CRITICAL",
                        "message": f"{code}: Cluster broker {h['pid']} grew {pct_prior:.1f}%→{pct_now:.1f}% (+{pct_now-pct_prior:.1f}pp)",
                    })
    
    # Alert 4: Float expanded >5% (lock-up deposit)
    total_now = today_data.get("total_shares", 0)
    total_prior = prior.get("total_shares", 0)
    if total_prior > 0 and (total_now - total_prior) / total_prior > 0.05:
        pct = (total_now - total_prior) / total_prior * 100
        alerts.append({
            "type": "FLOAT_EXPANSION",
            "severity": "MEDIUM",
            "message": f"{code}: Float expanded {pct:+.1f}% in 1 day ({total_prior:,}→{total_now:,})",
        })
    
    return alerts


# ── Main ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="CCASS Sentinel Daily Runner")
    parser.add_argument("--date", help="CCASS date (YYYY/MM/DD). Default: yesterday")
    parser.add_argument("--dry-run", action="store_true", help="Scrape but don't save")
    parser.add_argument(
        "--batch-write",
        type=int,
        default=BATCH_HOLDERS_WRITE,
        help="Write intermediate holders file every N collected stocks. Default: 100",
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
    parser.add_argument(
        "--auto-backfill",
        action="store_true",
        help="Run daily_backfill automatically if the daily run remains incomplete.",
    )
    args = parser.parse_args()
    
    # Date
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
    print(f"🛰️  CCASS Sentinel Daily Runner — {date_str}")
    batch_write = max(1, args.batch_write)
    stock_retries = max(0, args.stock_retries)
    timeout_rounds = max(0, args.timeout_rounds)
    
    # Refresh watchlist by discovering new listings
    print("\n  🔍 Refreshing watchlist...")
    discover_script = REPO_ROOT / "scripts" / "discover_new_listings.py"
    try:
        result = subprocess.run(
            [sys.executable, str(discover_script)],
            cwd=str(REPO_ROOT),
            capture_output=True,
            timeout=600,
        )
        if result.returncode == 0:
            print("  ✅ Watchlist refreshed successfully")
        else:
            stderr = result.stderr.decode('utf-8', errors='ignore')
            print(f"  ⚠️  Watchlist refresh had issues: {stderr[:200]}")
    except subprocess.TimeoutExpired:
        print("  ⚠️  Watchlist refresh timed out (taking too long)")
    except Exception as e:
        print(f"  ⚠️  Watchlist refresh failed: {e}")
    
    # Load watchlist
    if not WATCHLIST_FILE.exists():
        print(f"  ❌ No watchlist at {WATCHLIST_FILE}")
        sys.exit(1)
    
    watchlist = json.loads(WATCHLIST_FILE.read_text())
    codes = [w["code"] for w in watchlist]
    code_names = {w["code"]: w.get("name", "") for w in watchlist}
    print(f"  Watchlist: {len(codes)} stocks")
    
    # Load existing time-series
    if TIMESERIES_FILE.exists():
        ts = json.loads(TIMESERIES_FILE.read_text())
    else:
        ts = {}
    
    holders_file = HOLDERS_DIR / f"{date_key}.json"
    existing_holders = {}
    if holders_file.exists():
        try:
            existing_holders = json.loads(holders_file.read_text())
        except Exception:
            existing_holders = {}

    # Reconstruct metrics for codes that already have holders saved for this date.
    for code, holders in existing_holders.items():
        if code not in ts or date_key not in ts.get(code, {}):
            if holders:
                metrics = analyze(holders)
                metrics["date"] = date_key
                if code not in ts:
                    ts[code] = {}
                ts[code][date_key] = metrics

    targets = [c for c in codes if c not in existing_holders]

    if not targets:
        print(f"  ✅ Already collected {date_str} for all {len(codes)} stocks")
        return

    completed = len(codes) - len(targets)
    print(f"  Already have: {completed}/{len(codes)} stocks for {date_str}; collecting {len(targets)} missing stocks...")

    # Get ViewState
    vs = get_viewstate()
    if not vs:
        print("  ❌ Failed to get ViewState")
        sys.exit(1)
    
    # Scrape
    collected = 0
    skipped_no_data = 0
    skipped_error = 0
    all_alerts = []
    holders_lock = threading.Lock()
    daily_holders = {}
    pending_timeouts = []
    processed = 0

    def handle_result(code, result):
        nonlocal collected, skipped_no_data, skipped_error, processed, pending_timeouts
        status = result.get("status") if isinstance(result, dict) else None
        processed += 1

        if status == "SUCCESS":
            holders = result["holders"]
            metrics = analyze(holders)
            metrics["date"] = date_key

            if code not in ts:
                ts[code] = {}

            prior_dates = sorted(ts.get(code, {}).keys())
            if prior_dates:
                last_date = prior_dates[-1]
                if "holders" not in ts[code][last_date]:
                    holder_file = HOLDERS_DIR / f"{last_date}.json"
                    if holder_file.exists():
                        try:
                            hdata = json.loads(holder_file.read_text())
                            if code in hdata:
                                ts[code][last_date]["holders"] = hdata[code]
                        except:
                            pass

            ts[code][date_key] = metrics
            with holders_lock:
                if metrics.get("holders"):
                    daily_holders[code] = metrics.get("holders", [])
            collected += 1
            return

        if status == "NO_DATA":
            skipped_no_data += 1
            with holders_lock:
                daily_holders[code] = []
            return

        if status == "TIMEOUT":
            pending_timeouts.append(code)
            return

        skipped_error += 1
        if isinstance(result, dict) and result.get("message"):
            print(f"  ❌ {code}: {result['message']}")

    def run_fetch_round(codes_to_fetch):
        nonlocal processed
        if not codes_to_fetch:
            return []

        pending_timeouts.clear()
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {}
            for code in codes_to_fetch:
                f = pool.submit(scrape_stock, code, date_str, vs, stock_retries)
                futures[f] = code

            done_set = set()
            while futures:
                done, _ = wait(futures.keys() - done_set, return_when=FIRST_COMPLETED)
                for f in done:
                    done_set.add(f)
                    code = futures[f]
                    result = f.result()
                    handle_result(code, result)

                    if processed % 10 == 0:
                        print(f"  Progress: {processed} processed, {collected} collected, {skipped_error} errors, {len(pending_timeouts)} pending timeouts")

                    if collected > 0 and collected % batch_write == 0:
                        try:
                            HOLDERS_DIR.mkdir(parents=True, exist_ok=True)
                            holders_file = HOLDERS_DIR / f"{date_key}.json"
                            try:
                                existing = json.loads(holders_file.read_text()) if holders_file.exists() else {}
                            except Exception:
                                existing = {}

                            with holders_lock:
                                for k, v in daily_holders.items():
                                    if v or k not in existing:
                                        existing[k] = v

                            if existing:
                                tmp_holders = str(holders_file) + ".tmp"
                                with open(tmp_holders, "w") as f:
                                    json.dump(existing, f, ensure_ascii=False)
                                os.replace(tmp_holders, str(holders_file))
                                print(f"  💾 Interim holders saved ({len(existing)} entries)")
                        except Exception as e:
                            print(f"  ⚠️ Failed to flush holders: {e}")

                if done_set == set(futures.keys()):
                    break

        return list(pending_timeouts)

    targets_to_retry = run_fetch_round(targets)
    for round_num in range(timeout_rounds):
        if not targets_to_retry:
            break
        print(f"  🔁 Retrying {len(targets_to_retry)} timed-out stocks (round {round_num + 1}/{timeout_rounds})...")
        current = targets_to_retry
        pending_timeouts = []
        targets_to_retry = run_fetch_round(current)

    print(f"\n  ✅ Collected: {collected} | No data: {skipped_no_data} | Timeouts: {len(targets_to_retry)} | Errors: {skipped_error}")
    
    # Save
    if not args.dry_run:
        # Tier 2: Save full holders separately
        HOLDERS_DIR.mkdir(parents=True, exist_ok=True)
        holders_file = HOLDERS_DIR / f"{date_key}.json"
        # Merge incremental daily_holders with existing file, preserving non-empty existing entries
        existing_holders = {}
        if holders_file.exists():
            try:
                existing_holders = json.loads(holders_file.read_text())
            except Exception:
                existing_holders = {}

        with holders_lock:
            for code, h in daily_holders.items():
                if h:
                    existing_holders[code] = h
                else:
                    # only set empty if no existing value
                    if code not in existing_holders:
                        existing_holders[code] = h

        if existing_holders:
            tmp_holders = str(holders_file) + ".tmp"
            with open(tmp_holders, "w") as f:
                json.dump(existing_holders, f, ensure_ascii=False)
            os.replace(tmp_holders, str(holders_file))

        # Tier 1: Strip holders from timeseries (metrics only)
        for code in ts:
            for dk in ts[code]:
                ts[code][dk].pop("holders", None)
        
        # Atomic write
        tmp = str(TIMESERIES_FILE) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(ts, f, ensure_ascii=False)
        os.replace(tmp, str(TIMESERIES_FILE))
        print(f"  💾 Metrics → {TIMESERIES_FILE}")
        print(f"  💾 Holders → {holders_file}")
        print(f"  📊 Total: {len(ts)} stocks, {sum(len(v) for v in ts.values())} snapshots")
    
    # Alerts
    if all_alerts:
        print(f"\n  🚨 {len(all_alerts)} ALERTS:")
        for a in sorted(all_alerts, key=lambda x: {"CRITICAL":0,"HIGH":1,"MEDIUM":2}.get(x["severity"],3)):
            icon = {"CRITICAL":"🔴","HIGH":"🟠","MEDIUM":"🟡"}.get(a["severity"], "⚪")
            print(f"    {icon} [{a['type']}] {a['message']}")
        
        # Save alerts
        if not args.dry_run:
            alert_file = ALERTS_DIR / f"alerts_{date_key}.json"
            alert_file.parent.mkdir(parents=True, exist_ok=True)
            alert_file.write_text(json.dumps(all_alerts, indent=2, ensure_ascii=False))
            print(f"  📝 Alerts saved to {alert_file}")
    else:
        print(f"\n  ✅ No anomalies detected")
    
    # Summary stats
    print(f"\n  📈 DAILY SUMMARY:")
    highlights = []
    # Only highlight stocks with material BT5 change (>3pp) vs prior day
    for code in codes:
        if code not in ts or date_key not in ts[code]:
            continue
        d = ts[code][date_key]
        bt5 = d.get('broker_top5_pct', 0)
        # Find prior day
        sorted_dates = sorted(ts[code].keys())
        idx = sorted_dates.index(date_key) if date_key in sorted_dates else -1
        if idx > 0:
            prev_d = ts[code][sorted_dates[idx - 1]]
            prev_bt5 = prev_d.get('broker_top5_pct', 0)
            delta = bt5 - prev_bt5
            if abs(delta) >= 3.0:
                direction = "⬆" if delta > 0 else "⬇"
                name = code_names.get(code, "")[:8]
                highlights.append(f"{direction} {code} {name} BT5={bt5:.1f}% ({delta:+.1f}pp)")
                print(f"    {direction} {code}: BT5 {prev_bt5:.1f}%→{bt5:.1f}% ({delta:+.1f}pp)")
    if not highlights:
        print("    (no material BT5 moves today)")
    
    exit_code = 0
    if not args.dry_run and (targets_to_retry or skipped_error):
        print(f"\n  ⚠️ Incomplete daily run: {len(targets_to_retry)} timed-out stocks, {skipped_error} errors.")
        exit_code = 1
        if args.auto_backfill:
            backfill_script = Path(__file__).resolve().parent / "daily_backfill.py"
            if backfill_script.exists():
                bf_cmd = [sys.executable, str(backfill_script), "--date", date_str]
                bf_cmd += ["--stock-retries", str(stock_retries), "--timeout-rounds", str(timeout_rounds), "--batch-write", str(batch_write)]
                print("  🔧 Auto-backfilling missing stocks...")
                bf_result = subprocess.run(
                    bf_cmd,
                    cwd=str(REPO_ROOT),
                    capture_output=True,
                    timeout=600,
                )
                print(bf_result.stdout.decode("utf-8", errors="ignore"))
                if bf_result.returncode == 0:
                    print("  ✅ Auto-backfill completed successfully")
                    exit_code = 0
                else:
                    print("  ❌ Auto-backfill failed; check backfill logs for details")
                    print(bf_result.stderr.decode("utf-8", errors="ignore"))
            else:
                print(f"  ❌ Auto-backfill script not found: {backfill_script}")
    sys.exit(exit_code)
    
    # ── Telegram Push ──
    # if not args.dry_run:
    #     try:
    #         from telegram_push import push_daily_summary, push_alerts, push_error
    #         total_snaps = sum(len(v) for v in ts.values())
    #         push_daily_summary(date_key, collected, errors, len(ts), total_snaps, highlights)
    #         if all_alerts:
    #             push_alerts(date_key, all_alerts)
    #         print(f"  📱 Telegram push sent")
    #     except Exception as e:
    #         print(f"  ⚠️ Telegram push failed: {e}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"\n  💥 FATAL ERROR:\n{tb}")
        try:
            sys.path.insert(0, str(Path(__file__).resolve().parent))
            from telegram_push import push_error
            date_key = datetime.now().strftime("%Y-%m-%d")
            push_error(date_key, f"daily_runner.py crashed:\n{str(e)[:200]}")
            print(f"  📱 Error pushed to Telegram")
        except Exception as te:
            print(f"  ⚠️ Telegram error push also failed: {te}")
        sys.exit(1)
