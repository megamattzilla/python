#!/usr/bin/env python3
import sys
import argparse
import ipaddress
from collections import OrderedDict
import csv

def iter_ips(source):
    seen = set()
    for line in source:
        s = line.strip()
        if not s:
            continue
        try:
            ip = ipaddress.ip_address(s)
        except ValueError:
            # skip invalid lines
            continue
        if ip not in seen:
            seen.add(ip)
            yield s, ip  # keep original text + parsed object

def build_representatives(pairs):
    reps16 = OrderedDict()  # ip_network(/16) -> first ip_str seen
    reps24 = OrderedDict()  # ip_network(/24) -> first ip_str seen
    for ip_str, ip in pairs:
        n16 = ipaddress.ip_network(f"{ip}/16", strict=False)
        n24 = ipaddress.ip_network(f"{ip}/24", strict=False)
        reps16.setdefault(n16, ip_str)
        reps24.setdefault(n24, ip_str)
    return list(reps16.values()), list(reps24.values())

def write_single_column_csv(path, rows):
    # Writes just the IP per line, no header
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for val in rows:
            w.writerow([val])

def main():
    ap = argparse.ArgumentParser(
        description="Create two CSVs with one representative IP per unique /16 and /24 (IPs only, no masks)."
    )
    ap.add_argument("-i", "--input", help="Path to file of IPs (one per line). Omit for stdin.")
    ap.add_argument("--out16", default="subnets_16.csv", help="Output CSV for /16 reps (default: subnets_16.csv)")
    ap.add_argument("--out24", default="subnets_24.csv", help="Output CSV for /24 reps (default: subnets_24.csv)")
    args = ap.parse_args()

    # Read input
    if args.input:
        with open(args.input, "r", encoding="utf-8") as f:
            pairs = list(iter_ips(f))
    else:
        pairs = list(iter_ips(sys.stdin))

    # Build reps and write files
    reps16, reps24 = build_representatives(pairs)
    write_single_column_csv(args.out16, reps16)
    write_single_column_csv(args.out24, reps24)

    # Minimal status to stdout
    print(f"Wrote {len(reps16)} IPs to {args.out16} (/16 representatives)")
    print(f"Wrote {len(reps24)} IPs to {args.out24} (/24 representatives)")

if __name__ == "__main__":
    main()
