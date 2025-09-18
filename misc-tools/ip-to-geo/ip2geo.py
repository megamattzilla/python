#!/usr/bin/env python3
"""
Bulk IP -> country_code2 lookup using ipgeolocation.io v2 API.

Usage:
  python geo2csv.py --input ips.txt --output geo.csv --api-key YOUR_KEY
  # or with env var:
  export IPGEO_API_KEY=YOUR_KEY
  python geo2csv.py --input ips.txt --output geo.csv

Notes:
- Input file: one IP per line; blank lines and lines starting with '#' are ignored.
- Output CSV columns: ip,country_code2
"""

import argparse
import csv
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple, Optional, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


API_URL = "https://api.ipgeolocation.io/v2/ipgeo"


def make_session(timeout: int, max_retries: int) -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.request_timeout = timeout  # custom attr
    return s


def parse_ip_file(path: str) -> List[str]:
    ips = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            ips.append(line)
    # de-duplicate while preserving order
    seen = set()
    uniq = []
    for ip in ips:
        if ip not in seen:
            uniq.append(ip)
            seen.add(ip)
    return uniq


def fetch_country_code2(session: requests.Session, api_key: str, ip: str) -> Tuple[str, Optional[str]]:
    params = {"apiKey": api_key, "ip": ip}
    try:
        r = session.get(API_URL, params=params, timeout=session.request_timeout)
        # Some APIs return 200 with error payload; handle both
        if r.status_code != 200:
            return ip, None
        data = r.json()
        cc2 = None
        # Expected shape: data["location"]["country_code2"]
        loc = data.get("location")
        if isinstance(loc, dict):
            cc2 = loc.get("country_code2")
        # Fallbacks (just in case API variant returns country_code2 at root)
        if cc2 is None:
            cc2 = data.get("country_code2")
        # Normalize to string or None
        if isinstance(cc2, str):
            cc2 = cc2.strip()
        else:
            cc2 = None
        return ip, cc2
    except requests.RequestException:
        return ip, None
    except ValueError:
        # JSON decode error
        return ip, None


def main():
    parser = argparse.ArgumentParser(description="Resolve country_code2 for a list of IPs and write CSV.")
    parser.add_argument("--input", "-i", required=True, help="Path to input file (one IP per line).")
    parser.add_argument("--output", "-o", required=True, help="Path to output CSV file.")
    parser.add_argument("--api-key", help="ipgeolocation.io API key. If omitted, uses IPGEO_API_KEY env var.")
    parser.add_argument("--workers", type=int, default=8, help="Number of concurrent workers (default: 8).")
    parser.add_argument("--timeout", type=int, default=10, help="Per-request timeout in seconds (default: 10).")
    parser.add_argument("--retries", type=int, default=3, help="Max retries for transient errors (default: 3).")
    args = parser.parse_args()

    api_key = args.api_key or os.environ.get("IPGEO_API_KEY")
    if not api_key:
        print("ERROR: API key not provided. Use --api-key or set IPGEO_API_KEY.", file=sys.stderr)
        sys.exit(2)

    ips = parse_ip_file(args.input)
    if not ips:
        print("No IPs found in input.", file=sys.stderr)
        # Still write an empty CSV with header for consistency
        with open(args.output, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["ip", "country_code2"])
        sys.exit(0)

    session = make_session(timeout=args.timeout, max_retries=args.retries)

    results: List[Tuple[str, Optional[str]]] = []
    errors = 0

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {executor.submit(fetch_country_code2, session, api_key, ip): ip for ip in ips}
        for fut in as_completed(futures):
            ip, cc2 = fut.result()
            if cc2 is None:
                errors += 1
            results.append((ip, cc2 or ""))

    # Maintain original order in output
    order = {ip: idx for idx, ip in enumerate(ips)}
    results.sort(key=lambda t: order.get(t[0], 10**9))

    with open(args.output, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["ip", "country_code2"])
        writer.writerows(results)

    if errors:
        print(f"Completed with {errors} lookup error(s). Missing values left blank.", file=sys.stderr)


if __name__ == "__main__":
    main()
