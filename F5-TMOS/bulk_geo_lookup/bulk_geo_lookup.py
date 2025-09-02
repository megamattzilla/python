#!/usr/bin/env python3
import argparse
import concurrent.futures as cf
import csv
import os
import re
import subprocess
import sys
from typing import Tuple, Optional, List

## Regex pattern to capture "country_code = XX" from geoip_lookup output
COUNTRY_RE = re.compile(r"^\s*country_code\s*=\s*([A-Z]{2})\s*$", re.M)

## Regex pattern to capture an 8-digit DB version string (YYYYMMDD) from output
VERSION_RE = re.compile(r"version\s*=.*\b(\d{8})\b")

def get_db_version(cmd: str, timeout: float) -> str:
    """
    ## Run geoip_lookup once to detect the database version.
    ## This helps us label the CSV column with the DB version in use.
    """
    try:
        ## Call geoip_lookup on a harmless IP (1.1.1.1)
        out = subprocess.run(
            [cmd, "1.1.1.1"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,  ## suppress errors
            text=True,
            timeout=timeout,
            check=False,
        ).stdout

        ## Try to find the version in the output
        m = VERSION_RE.search(out or "")
        return m.group(1) if m else "UNKNOWN"
    except Exception:
        ## If something fails, return UNKNOWN
        return "UNKNOWN"

def run_geoip_lookup(ip: str, cmd: str, timeout: float) -> Tuple[str, str]:
    """
    ## Run geoip_lookup for one IP and return (ip, country_code).
    ## If lookup fails, return "UNKNOWN".
    """
    if not ip or ip.startswith("#"):
        ## Skip empty or commented lines
        return ip, "UNKNOWN"

    try:
        ## Run the geoip_lookup command
        out = subprocess.run(
            [cmd, ip],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=timeout,
            check=False,
        ).stdout

        ## Extract the country code from output
        m = COUNTRY_RE.search(out or "")
        return ip, (m.group(1) if m else "UNKNOWN")
    except subprocess.TimeoutExpired:
        ## Timeout → mark as UNKNOWN
        return ip, "UNKNOWN"
    except Exception:
        ## Any other error → mark as UNKNOWN
        return ip, "UNKNOWN"

def read_ips(path: str) -> List[str]:
    """
    ## Read all IP addresses from a file.
    ## Strips whitespace and ignores blank lines.
    """
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return [line.strip() for line in f if line.strip()]

def main():
    ## Setup command line arguments for the script
    p = argparse.ArgumentParser(description="Parallel geoip_lookup -> CSV")
    p.add_argument("-i", "--input", required=True, help="Input file with one IP per line")
    p.add_argument("-o", "--output", default="-", help="Output CSV file (default: stdout)")
    p.add_argument("--cmd", default="geoip_lookup", help="geoip lookup command (default: geoip_lookup)")
    p.add_argument("-c", "--concurrency", type=int,
                   default=max(8, (os.cpu_count() or 2) * 5),
                   help="Number of parallel workers (default: ~5x CPU cores)")
    p.add_argument("--timeout", type=float, default=3.0,
                   help="Per-IP command timeout in seconds (default: 3.0)")
    args = p.parse_args()

    ## Read all IPs from the input file
    ips = read_ips(args.input)
    if not ips:
        ## If no IPs, just print a simple header and exit
        print("ip,country")
        return

    ## Detect the DB version (for CSV column header)
    db_version = get_db_version(args.cmd, args.timeout)

    ## Decide where to write output (stdout or file)
    outfh = sys.stdout if args.output == "-" else open(args.output, "w", newline="", encoding="utf-8")

    try:
        writer = csv.writer(outfh)
        ## Write the CSV header, including the DB version
        writer.writerow(["ip", f"country_{db_version}"])

        ## Create a thread pool to run lookups in parallel
        with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
            ## Submit each IP lookup as a separate task
            futures = [(idx, ex.submit(run_geoip_lookup, ip, args.cmd, args.timeout))
                       for idx, ip in enumerate(ips)]

            ## Collect results in the same order as the input file
            results: List[Optional[Tuple[str, str]]] = [None] * len(futures)
            for idx, fut in futures:
                ip, country = fut.result()
                results[idx] = (ip, country)

        ## Write each (ip, country) row to CSV
        for ip, country in results:
            writer.writerow([ip, country])

    finally:
        ## Close the file if we opened one
        if outfh is not sys.stdout:
            outfh.close()

if __name__ == "__main__":
    main()
