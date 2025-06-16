#!/usr/bin/env python3
"""
generate_synthetic_csv.py

Usage:
    python generate_synthetic_csv.py -i base.csv -o synthetic.csv -n 100

Description:
    Reads a base CSV file with a single job entry and generates a synthetic
    CSV file with N jobs by incrementing the job_name's numeric suffix.
"""

import csv
import re
import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic CSV with N jobs.")
    parser.add_argument('-i', '--input', required=True, help="Path to base CSV file")
    parser.add_argument('-o', '--output', required=True, help="Path to output CSV file")
    parser.add_argument('-n', '--num', type=int, required=True, help="Number of jobs to generate")
    return parser.parse_args()


def split_name(name):
    m = re.match(r'^(.*?)(\d+)$', name)
    if m:
        prefix, num_str = m.groups()
        width = len(num_str)
        start = int(num_str)
        return prefix, start, width
    else:
        # No numeric suffix: start numbering at 1 without zero-padding
        return name, 1, 0


def main():
    args = parse_args()
    # Read base CSV
    with open(args.input, newline='') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        rows = list(reader)
        if not rows:
            raise ValueError("Input CSV must have at least one data row.")
        base_row = rows[0]

    # Parse job_name into prefix and starting index
    prefix, start, width = split_name(base_row['job_name'])

    # Write synthetic CSV
    with open(args.output, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for i in range(args.num):
            idx = start + i
            if width:
                job_name = f"{prefix}{str(idx).zfill(width)}"
            else:
                job_name = f"{prefix}{idx}"
            new_row = base_row.copy()
            new_row['job_name'] = job_name
            writer.writerow(new_row)


if __name__ == '__main__':
    main()
