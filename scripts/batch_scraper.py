#!/usr/bin/env python3
"""Batch CCASS Scraper

Scrapes CCASS data for a date range, executing one day at a time.
Automatically skips weekends (Sat/Sun).

Usage:
    python batch_scraper.py                                    # 2025/06/03 - 2026/05/24
    python batch_scraper.py --start 2025/06/03 --end 2026/05/24

    python scripts/batch_scraper.py --start 2025/06/03 --end 2026/06/30 

    python scripts/batch_scraper.py --start 2025/06/03 --end 2026/05/24

    python batch_scraper.py --start 2025/06/03 --end 2026/05/24 --dry-run

    python batch_scraper.py --start 2025/06/03 --end 2026/05/24 --log-dir logs/daily_runner
"""

import argparse
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DAILY_RUNNER = REPO_ROOT / "scripts" / "daily_runner.py"

def generate_weekday_dates(start_date_str, end_date_str):
    """Generate all weekdays (Mon-Fri) between two dates (inclusive)."""
    start = datetime.strptime(start_date_str, "%Y/%m/%d")
    end = datetime.strptime(end_date_str, "%Y/%m/%d")
    
    dates = []
    current = start
    while current <= end:
        # weekday(): 0=Mon, 1=Tue, ..., 4=Fri, 5=Sat, 6=Sun
        if current.weekday() < 5:  # Monday to Friday
            dates.append(current)
        current += timedelta(days=1)
    
    return dates

def write_log(path, cmd, returncode, stdout, stderr, timeout=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'wb') as f:
        f.write(f"Command: {' '.join(cmd)}\n".encode('utf-8'))
        if returncode is not None:
            f.write(f"Exit code: {returncode}\n".encode('utf-8'))
        if timeout is not None:
            f.write(f"Timeout: {timeout}s\n".encode('utf-8'))
        f.write(b"\nSTDOUT:\n")
        f.write(stdout or b"")
        f.write(b"\n\nSTDERR:\n")
        f.write(stderr or b"")


def main():
    parser = argparse.ArgumentParser(
        description="Batch scrape CCASS data for a date range"
    )
    parser.add_argument(
        "--start",
        default="2025/06/03",
        help="Start date (YYYY/MM/DD). Default: 2025/06/03"
    )
    parser.add_argument(
        "--end",
        default="2026/05/24",
        help="End date (YYYY/MM/DD). Default: 2026/05/24"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scrape but don't save"
    )
    parser.add_argument(
        "--log-dir",
        default="logs/daily_runner",
        help="Directory to store per-day daily_runner logs"
    )
    parser.add_argument(
        "--daily-timeout",
        type=int,
        default=0,
        help="Per-date daily_runner timeout in seconds. Default 0 = no timeout."
    )
    args = parser.parse_args()
    
    # Validate daily_runner exists
    if not DAILY_RUNNER.exists():
        print(f"❌ daily_runner.py not found at {DAILY_RUNNER}")
        sys.exit(1)
    
    # Generate trading dates
    dates = generate_weekday_dates(args.start, args.end)
    total = len(dates)
    
    print(f"🛰️  Batch CCASS Scraper")
    print(f"  Range: {args.start} to {args.end}")
    print(f"  Trading dates (weekdays): {total}")
    print(f"  Mode: {'DRY-RUN' if args.dry_run else 'SAVE'}\n")
    
    # Run daily_runner for each date
    success = 0
    failed = 0
    failed_dates = []
    
    # Retry policy for broad per-day timeouts
    MAX_RETRIES = 3
    if args.daily_timeout > 0:
        TIMEOUTS = [args.daily_timeout, args.daily_timeout * 2, args.daily_timeout * 3]
    else:
        TIMEOUTS = [None]
    logs_dir = REPO_ROOT / args.log_dir
    logs_dir.mkdir(parents=True, exist_ok=True)

    for i, date in enumerate(dates, 1):
        date_str = date.strftime("%Y/%m/%d")

        # Build command using the current Python interpreter
        cmd = [sys.executable, str(DAILY_RUNNER), "--date", date_str]
        if args.dry_run:
            cmd.append("--dry-run")
        else:
            cmd.append("--auto-backfill")

        print(f"[{i}/{total}] {date_str} ({date.strftime('%a')})...", end=" ", flush=True)

        attempt = 0
        succeeded = False

        while attempt < MAX_RETRIES and not succeeded:
            attempt += 1
            to = TIMEOUTS[min(attempt - 1, len(TIMEOUTS) - 1)]
            try:
                result = subprocess.run(
                    cmd,
                    cwd=str(REPO_ROOT),
                    capture_output=True,
                    timeout=to,
                )

                log_path = logs_dir / f"{date.strftime('%Y-%m-%d')}.log"
                if result.returncode == 0:
                    print("✅")
                    success += 1
                    succeeded = True
                    write_log(log_path, cmd, result.returncode, result.stdout, result.stderr)
                    break
                else:
                    # Non-zero exit — treat as failure for this date
                    stderr = result.stderr.decode('utf-8', errors='ignore')
                    print("❌")
                    print(f"     Error: {stderr[:200]}")
                    write_log(log_path, cmd, result.returncode, result.stdout, result.stderr)
                    break

            except subprocess.TimeoutExpired as e:
                # On timeout, retry with a longer timeout up to MAX_RETRIES
                last_err = 'TIMEOUT'
                if attempt < MAX_RETRIES:
                    print(f"⏱️  (TIMEOUT) — retrying (attempt {attempt + 1}/{MAX_RETRIES})")
                else:
                    print("⏱️  (TIMEOUT)")
                    log_path = logs_dir / f"{date.strftime('%Y-%m-%d')}.log"
                    write_log(log_path, cmd, None, getattr(e, 'stdout', b''), getattr(e, 'stderr', b''), timeout=to)
            except Exception as e:
                print(f"❌ ({e})")
                last_err = e
                log_path = logs_dir / f"{date.strftime('%Y-%m-%d')}.log"
                write_log(log_path, cmd, None, b'', str(e).encode('utf-8'))
                break

        if not succeeded:
            failed += 1
            if date_str not in failed_dates:
                failed_dates.append(date_str)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"📊 BATCH SUMMARY")
    print(f"  Success: {success}/{total}")
    print(f"  Failed:  {failed}/{total}")
    
    if failed_dates:
        print(f"\n  Failed dates ({len(failed_dates)}):")
        for d in failed_dates:
            print(f"    - {d}")
    
    sys.exit(0 if failed == 0 else 1)

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
