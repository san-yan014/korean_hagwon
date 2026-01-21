"""
approximate dates from relative times like "2 days ago".
no api calls needed - just math based on scrape date.

usage:
    python approximate_dates.py comments.csv --scrape-date 2026-01-20
"""

import argparse
import csv
import re
from datetime import datetime, timedelta


def parse_relative_date(time_str, scrape_date):
    """convert '2 days ago' to approximate date"""
    if not time_str:
        return None
    
    # already an iso date? skip
    if 'T' in str(time_str) and '-' in str(time_str):
        return time_str
    
    time_str = str(time_str).lower().strip().replace('(edited)', '').strip()
    
    match = re.match(r'(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago', time_str)
    if not match:
        if 'just now' in time_str or 'moment' in time_str:
            return scrape_date.strftime('%Y-%m-%dT%H:%M:%SZ')
        return None
    
    num = int(match.group(1))
    unit = match.group(2)
    
    if unit == 'second':
        approx = scrape_date - timedelta(seconds=num)
    elif unit == 'minute':
        approx = scrape_date - timedelta(minutes=num)
    elif unit == 'hour':
        approx = scrape_date - timedelta(hours=num)
    elif unit == 'day':
        approx = scrape_date - timedelta(days=num)
    elif unit == 'week':
        approx = scrape_date - timedelta(weeks=num)
    elif unit == 'month':
        approx = scrape_date - timedelta(days=num * 30)
    elif unit == 'year':
        approx = scrape_date - timedelta(days=num * 365)
    else:
        return None
    
    return approx.strftime('%Y-%m-%dT%H:%M:%SZ')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('csv_file', help='comments csv')
    parser.add_argument('--scrape-date', required=True, help='date when scraped (YYYY-MM-DD)')
    parser.add_argument('--output', help='output file (default: overwrites input)')
    args = parser.parse_args()
    
    scrape_date = datetime.strptime(args.scrape_date, '%Y-%m-%d')
    
    # load csv
    with open(args.csv_file, 'r', encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))
    
    print(f"loaded {len(rows)} comments")
    
    # convert dates
    converted = 0
    already_done = 0
    failed = 0
    
    for row in rows:
        date = row.get('date', '')
        
        # skip if already iso format
        if date and 'T' in date and '-' in date and 'ago' not in date.lower():
            already_done += 1
            continue
        
        approx = parse_relative_date(date, scrape_date)
        if approx:
            row['date'] = approx
            converted += 1
        else:
            failed += 1
    
    print(f"converted: {converted}")
    print(f"already done: {already_done}")
    print(f"failed: {failed}")
    
    # save
    output_file = args.output or args.csv_file
    fieldnames = list(rows[0].keys())
    
    with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"saved to {output_file}")


if __name__ == '__main__':
    main()