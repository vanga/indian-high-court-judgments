#!/usr/bin/env python3
"""
Script to analyze Indian High Court Judgments data index files and generate statistics.

This script reads index files from local directory and generates comprehensive statistics by year and court.
It processes both metadata.index.json and data.index.json files to provide insights into
the dataset size, distribution, and trends.

Usage:
    # Generate all statistics
    python stats.py

    # Generate statistics for specific year
    python stats.py --year 2024

    # Generate statistics for specific court
    python stats.py --court 27_1

    # Output to specific format
    python stats.py --output-format csv
    python stats.py --output-format json
"""

import argparse
import json
import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

from court_utils import get_court_codes, get_bench_codes, to_s3_format


class CourtJudgmentStats:
    """Class to analyze and generate statistics from Indian High Court Judgments data."""

    def __init__(self, data_dir: str = "../output/data/tar"):
        self.data_dir = Path(data_dir)
        self.court_codes = get_court_codes()
        self.bench_codes = get_bench_codes()

        # Statistics storage with proper type annotations
        self.stats_by_year: Dict[int, Dict] = defaultdict(lambda: {
            'total_files': 0,
            'total_size': 0,
            'courts': set(),
            'benches': set(),
            'metadata_files': 0,
            'data_files': 0,
            'metadata_size': 0,
            'data_size': 0
        })

        self.stats_by_court: Dict[str, Dict] = defaultdict(lambda: {
            'total_files': 0,
            'total_size': 0,
            'years': set(),
            'benches': set(),
            'metadata_files': 0,
            'data_files': 0,
            'metadata_size': 0,
            'data_size': 0
        })

        self.stats_by_bench: Dict[str, Dict] = defaultdict(lambda: {
            'total_files': 0,
            'total_size': 0,
            'years': set(),
            'courts': set(),
            'metadata_files': 0,
            'data_files': 0,
            'metadata_size': 0,
            'data_size': 0
        })

    def load_index_file(self, year: int, court_code: str, bench: str, file_type: str) -> Optional[Dict]:
        """Load index file from local directory for a specific court/bench/year combination."""
        court_code_s3 = to_s3_format(court_code)
        index_path = self.data_dir / \
            f"year={year}" / f"court={court_code_s3}" / \
            f"bench={bench}" / f"{file_type}.index.json"

        try:
            with open(index_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Could not load {index_path}: {e}")
            return None

    def process_index_file(self, year: int, court_code: str, bench: str, file_type: str):
        """Process a single index file and update statistics."""
        index_data = self.load_index_file(year, court_code, bench, file_type)
        if not index_data:
            return

        # Extract statistics from index file
        file_count = index_data.get('file_count', 0)
        tar_size = index_data.get('tar_size', 0)

        # Update year statistics
        self.stats_by_year[year]['total_files'] += file_count
        self.stats_by_year[year]['total_size'] += tar_size
        self.stats_by_year[year]['courts'].add(court_code)
        self.stats_by_year[year]['benches'].add(bench)

        if file_type == 'metadata':
            self.stats_by_year[year]['metadata_files'] += file_count
            self.stats_by_year[year]['metadata_size'] += tar_size
        else:
            self.stats_by_year[year]['data_files'] += file_count
            self.stats_by_year[year]['data_size'] += tar_size

        # Update court statistics
        self.stats_by_court[court_code]['total_files'] += file_count
        self.stats_by_court[court_code]['total_size'] += tar_size
        self.stats_by_court[court_code]['years'].add(year)
        self.stats_by_court[court_code]['benches'].add(bench)

        if file_type == 'metadata':
            self.stats_by_court[court_code]['metadata_files'] += file_count
            self.stats_by_court[court_code]['metadata_size'] += tar_size
        else:
            self.stats_by_court[court_code]['data_files'] += file_count
            self.stats_by_court[court_code]['data_size'] += tar_size

        # Update bench statistics
        self.stats_by_bench[bench]['total_files'] += file_count
        self.stats_by_bench[bench]['total_size'] += tar_size
        self.stats_by_bench[bench]['years'].add(year)
        self.stats_by_bench[bench]['courts'].add(court_code)

        if file_type == 'metadata':
            self.stats_by_bench[bench]['metadata_files'] += file_count
            self.stats_by_bench[bench]['metadata_size'] += tar_size
        else:
            self.stats_by_bench[bench]['data_files'] += file_count
            self.stats_by_bench[bench]['data_size'] += tar_size

    def discover_available_data(self) -> Tuple[Set[int], Set[str], Dict[str, Set[str]]]:
        """Discover available years, courts, and benches from the local directory structure."""
        years = set()
        courts = set()
        court_benches = defaultdict(set)

        if not self.data_dir.exists():
            print(f"Warning: Data directory {self.data_dir} does not exist")
            return years, courts, court_benches

        # Scan year directories
        for year_dir in self.data_dir.iterdir():
            if year_dir.is_dir() and year_dir.name.startswith('year='):
                try:
                    year = int(year_dir.name.split('=')[1])
                    years.add(year)

                    # Scan court directories
                    for court_dir in year_dir.iterdir():
                        if court_dir.is_dir() and court_dir.name.startswith('court='):
                            court_code = court_dir.name.split('=')[1]
                            courts.add(court_code)

                            # Scan bench directories
                            for bench_dir in court_dir.iterdir():
                                if bench_dir.is_dir() and bench_dir.name.startswith('bench='):
                                    bench_name = bench_dir.name.split('=')[1]
                                    court_benches[court_code].add(bench_name)
                except ValueError:
                    continue

        return years, courts, court_benches

    def analyze_data(self, year_filter: Optional[int] = None, court_filter: Optional[str] = None):
        """Analyze all available index files and generate statistics."""
        print("Discovering available data...")
        years, courts, court_benches = self.discover_available_data()

        if not years:
            print("No data found in the specified directory")
            return

        print(f"Found data for {len(years)} years, {len(courts)} courts")

        # Filter years and courts if specified
        years_to_analyze = {year_filter} if year_filter else years
        courts_to_analyze = {court_filter} if court_filter else courts

        total_combinations = 0
        processed_combinations = 0

        for year in years_to_analyze:
            for court_code in courts_to_analyze:
                if court_code not in court_benches:
                    continue

                for bench in court_benches[court_code]:
                    total_combinations += 2  # metadata and data files

                    # Process metadata index
                    self.process_index_file(
                        year, court_code, bench, 'metadata')
                    processed_combinations += 1

                    # Process data index
                    self.process_index_file(year, court_code, bench, 'data')
                    processed_combinations += 1

                    if processed_combinations % 100 == 0:
                        print(
                            f"Processed {processed_combinations}/{total_combinations} combinations...")

        print(
            f"Analysis complete. Processed {processed_combinations} index files.")

    def format_size(self, size_bytes: int) -> str:
        """Convert bytes to human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"

    def get_court_name(self, court_code: str) -> str:
        """Get human readable court name from court code."""
        return self.court_codes.get(court_code, court_code)

    def generate_year_statistics(self) -> List[Dict]:
        """Generate statistics by year."""
        stats = []
        for year in sorted(self.stats_by_year.keys()):
            year_data = self.stats_by_year[year]
            stats.append({
                'year': year,
                'total_files': year_data['total_files'],
                'total_size': year_data['total_size'],
                'total_size_human': self.format_size(year_data['total_size']),
                'courts_count': len(year_data['courts']),
                'benches_count': len(year_data['benches']),
                'metadata_files': year_data['metadata_files'],
                'data_files': year_data['data_files'],
                'metadata_size': year_data['metadata_size'],
                'metadata_size_human': self.format_size(year_data['metadata_size']),
                'data_size': year_data['data_size'],
                'data_size_human': self.format_size(year_data['data_size'])
            })
        return stats

    def generate_court_statistics(self) -> List[Dict]:
        """Generate statistics by court."""
        stats = []
        for court_code in sorted(self.stats_by_court.keys()):
            court_data = self.stats_by_court[court_code]
            stats.append({
                'court_code': court_code,
                'court_name': self.get_court_name(court_code),
                'total_files': court_data['total_files'],
                'total_size': court_data['total_size'],
                'total_size_human': self.format_size(court_data['total_size']),
                'years_count': len(court_data['years']),
                'benches_count': len(court_data['benches']),
                'years': sorted(list(court_data['years'])),
                'benches': sorted(list(court_data['benches'])),
                'metadata_files': court_data['metadata_files'],
                'data_files': court_data['data_files'],
                'metadata_size': court_data['metadata_size'],
                'metadata_size_human': self.format_size(court_data['metadata_size']),
                'data_size': court_data['data_size'],
                'data_size_human': self.format_size(court_data['data_size'])
            })
        return stats

    def generate_bench_statistics(self) -> List[Dict]:
        """Generate statistics by bench."""
        stats = []
        for bench in sorted(self.stats_by_bench.keys()):
            bench_data = self.stats_by_bench[bench]
            court_code = self.bench_codes.get(bench, 'unknown')
            stats.append({
                'bench_name': bench,
                'court_code': court_code,
                'court_name': self.get_court_name(court_code),
                'total_files': bench_data['total_files'],
                'total_size': bench_data['total_size'],
                'total_size_human': self.format_size(bench_data['total_size']),
                'years_count': len(bench_data['years']),
                'courts_count': len(bench_data['courts']),
                'years': sorted(list(bench_data['years'])),
                'courts': sorted(list(bench_data['courts'])),
                'metadata_files': bench_data['metadata_files'],
                'data_files': bench_data['data_files'],
                'metadata_size': bench_data['metadata_size'],
                'metadata_size_human': self.format_size(bench_data['metadata_size']),
                'data_size': bench_data['data_size'],
                'data_size_human': self.format_size(bench_data['data_size'])
            })
        return stats

    def save_to_csv(self, filename: str, data: List[Dict]):
        """Save statistics to CSV file."""
        if not data:
            return

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

    def save_to_json(self, filename: str, data: Dict):
        """Save statistics to JSON file."""
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, indent=2, ensure_ascii=False)

    def print_summary(self):
        """Print a summary of the statistics."""
        print("\n" + "="*80)
        print("INDIAN HIGH COURT JUDGMENTS - DATASET STATISTICS")
        print("="*80)

        # Overall statistics
        total_files = sum(stats['total_files']
                          for stats in self.stats_by_year.values())
        total_size = sum(stats['total_size']
                         for stats in self.stats_by_year.values())
        total_courts = len(self.stats_by_court)
        total_benches = len(self.stats_by_bench)
        years_covered = len(self.stats_by_year)

        print(f"\nOVERALL STATISTICS:")
        print(f"  Total Files: {total_files:,}")
        print(f"  Total Size: {self.format_size(total_size)}")
        print(f"  Courts: {total_courts}")
        print(f"  Benches: {total_benches}")
        print(f"  Years Covered: {years_covered}")

        # Top courts by file count
        court_stats = self.generate_court_statistics()
        top_courts = sorted(
            court_stats, key=lambda x: x['total_files'], reverse=True)[:5]

        print(f"\nTOP 5 COURTS BY FILE COUNT:")
        for i, court in enumerate(top_courts, 1):
            print(
                f"  {i}. {court['court_name']} ({court['court_code']}): {court['total_files']:,} files, {court['total_size_human']}")

        # Year distribution
        year_stats = self.generate_year_statistics()
        print(f"\nYEAR DISTRIBUTION:")
        for year_stat in year_stats:
            print(
                f"  {year_stat['year']}: {year_stat['total_files']:,} files, {year_stat['total_size_human']}")

    def generate_all_statistics(self, output_format: str = 'csv'):
        """Generate all statistics and save to files."""
        # Generate statistics
        year_stats = self.generate_year_statistics()
        court_stats = self.generate_court_statistics()
        bench_stats = self.generate_bench_statistics()

        # Prepare output data
        output_data = {
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_files': sum(stats['total_files'] for stats in year_stats),
                'total_size': sum(stats['total_size'] for stats in year_stats),
                'total_courts': len(court_stats),
                'total_benches': len(bench_stats),
                'years_covered': len(year_stats)
            },
            'by_year': year_stats,
            'by_court': court_stats,
            'by_bench': bench_stats
        }

        if output_format == 'csv':
            self.save_to_csv('stats_by_year.csv', year_stats)
            self.save_to_csv('stats_by_court.csv', court_stats)
            self.save_to_csv('stats_by_bench.csv', bench_stats)
            print(f"\nStatistics saved to CSV files:")
            print(f"  - stats_by_year.csv")
            print(f"  - stats_by_court.csv")
            print(f"  - stats_by_bench.csv")

        elif output_format == 'json':
            self.save_to_json('stats_complete.json', output_data)
            print(f"\nComplete statistics saved to: stats_complete.json")

        return output_data


def main():
    parser = argparse.ArgumentParser(
        description='Generate statistics for Indian High Court Judgments dataset')
    parser.add_argument('--year', type=int, help='Filter by specific year')
    parser.add_argument('--court', type=str,
                        help='Filter by specific court code')
    parser.add_argument('--output-format', choices=['csv', 'json'], default='csv',
                        help='Output format (default: csv)')
    parser.add_argument('--data-dir', default='../output/data/tar',
                        help='Local data directory (default: ../output/data/tar)')

    args = parser.parse_args()

    # Initialize statistics analyzer
    stats_analyzer = CourtJudgmentStats(data_dir=args.data_dir)

    # Analyze data
    stats_analyzer.analyze_data(year_filter=args.year, court_filter=args.court)

    # Generate and save statistics
    output_data = stats_analyzer.generate_all_statistics(
        output_format=args.output_format)

    # Print summary
    stats_analyzer.print_summary()


if __name__ == "__main__":
    main()
