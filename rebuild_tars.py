#!/usr/bin/env python3
"""
Rebuild TAR files from individual S3 files

This script rebuilds TAR archives from individual PDF and JSON files stored in S3,
removing any duplicates and ensuring clean structure. It will:

1. Download individual files from S3 (metadata/json/ and data/pdf/)
2. Create fresh TAR archives with no duplicates
3. Delete old TAR files from S3
4. Upload new clean TAR files

After running this script, run update_index_files.py to update the index files.

Usage:
    python rebuild_tars.py                    # Process all courts/benches
    python rebuild_tars.py --court 27_1       # Process specific court
    python rebuild_tars.py --bench hcbgoa     # Process specific bench
    
    # Then update index files:
    python update_index_files.py
"""

import json
import boto3
import os
import tarfile
import tempfile
import shutil
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple
from tqdm import tqdm
from botocore import UNSIGNED
from botocore.client import Config


class TARRebuilder:
    """Rebuild TAR files from individual S3 files, removing duplicates"""
    
    def __init__(self, bucket_name: str, year: str = None):
        """
        Initialize the TAR rebuilder
        
        Args:
            bucket_name: S3 bucket name
            year: Year to process (defaults to current year)
        """
        self.bucket_name = bucket_name
        self.year = year or str(datetime.now().year)
        
        # Initialize S3 clients
        self.s3_client = boto3.client('s3')
        self.s3_unsigned = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        
        print(f"Initialized TARRebuilder for bucket: {bucket_name}, year: {self.year}")
    
    def _format_size(self, size_bytes: int) -> str:
        """Convert bytes to human readable format"""
        if size_bytes == 0:
            return "0 B"
        
        size_units = ["B", "KB", "MB", "GB", "TB"]
        size = float(size_bytes)
        unit_index = 0
        
        while size >= 1024.0 and unit_index < len(size_units) - 1:
            size /= 1024.0
            unit_index += 1
        
        if unit_index == 0:
            return f"{int(size)} {size_units[unit_index]}"
        else:
            return f"{size:.2f} {size_units[unit_index]}"
    
    def _list_s3_files(self, prefix: str, extension: str = None) -> List[str]:
        """List all files with given prefix"""
        files = []
        paginator = self.s3_unsigned.get_paginator('list_objects_v2')
        
        try:
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if extension is None or key.endswith(extension):
                            files.append(key)
        except Exception as e:
            print(f"Error listing files with prefix {prefix}: {e}")
        
        return files
    
    def _download_file(self, s3_key: str, local_path: str) -> bool:
        """Download a file from S3 to local path"""
        try:
            # Create parent directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            self.s3_unsigned.download_file(self.bucket_name, s3_key, local_path)
            return True
        except Exception as e:
            print(f"Error downloading {s3_key}: {e}")
            return False
    
    def _delete_s3_file(self, s3_key: str) -> bool:
        """Delete a file from S3"""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            print(f"  Deleted old TAR: {s3_key}")
            return True
        except Exception as e:
            print(f"  Error deleting {s3_key}: {e}")
            return False
    
    def _upload_file_to_s3(self, local_path: str, s3_key: str, content_type: str) -> bool:
        """Upload a file to S3 with progress"""
        try:
            file_size = os.path.getsize(local_path)
            print(f"  Uploading {self._format_size(file_size)} to {s3_key}")
            
            # For files < 5GB, use simple upload
            if file_size < 5 * 1024 * 1024 * 1024:
                with open(local_path, 'rb') as f:
                    self.s3_client.put_object(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        Body=f,
                        ContentType=content_type
                    )
                print(f"  Upload complete")
                return True
            else:
                # For large files, use multipart upload with 5GB chunks
                print(f"  Using multipart upload for large file")
                import math
                
                chunk_size = 5 * 1024 * 1024 * 1024  # 5GB chunks
                total_parts = math.ceil(file_size / chunk_size)
                
                mpu = self.s3_client.create_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    ContentType=content_type
                )
                upload_id = mpu['UploadId']
                
                parts = []
                part_number = 1
                
                with open(local_path, 'rb') as f:
                    with tqdm(total=total_parts, desc="  Uploading parts", unit="part") as pbar:
                        while True:
                            chunk = f.read(chunk_size)
                            if not chunk:
                                break
                            
                            part_response = self.s3_client.upload_part(
                                Bucket=self.bucket_name,
                                Key=s3_key,
                                PartNumber=part_number,
                                UploadId=upload_id,
                                Body=chunk
                            )
                            
                            parts.append({
                                'PartNumber': part_number,
                                'ETag': part_response['ETag']
                            })
                            
                            part_number += 1
                            pbar.update(1)
                
                self.s3_client.complete_multipart_upload(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )
                
                print(f"  Multipart upload complete")
                return True
                
        except Exception as e:
            print(f"  Error uploading {s3_key}: {e}")
            return False
    
    def _create_tar_from_files(self, files: List[str], temp_dir: str, tar_filename: str, 
                               compress: bool = False) -> Tuple[str, int]:
        """
        Create TAR file from list of files
        
        Returns:
            Tuple of (tar_path, tar_size)
        """
        tar_path = os.path.join(temp_dir, tar_filename)
        mode = 'w:gz' if compress else 'w'
        
        # Track unique files by basename to avoid duplicates
        unique_files = {}
        for file_path in files:
            basename = os.path.basename(file_path)
            if basename not in unique_files:
                unique_files[basename] = file_path
        
        print(f"  Creating TAR with {len(unique_files)} unique files (removed {len(files) - len(unique_files)} duplicates)")
        
        with tarfile.open(tar_path, mode) as tar:
            for basename, file_path in tqdm(unique_files.items(), desc="  Adding files to TAR", unit="file"):
                if os.path.exists(file_path):
                    # Add file with just the basename (no directory structure)
                    tar.add(file_path, arcname=basename)
        
        tar_size = os.path.getsize(tar_path)
        print(f"  TAR created: {self._format_size(tar_size)}")
        
        return tar_path, tar_size
    
    def _update_index_file(self, court_code: str, bench: str, file_type: str, 
                          files: List[str], tar_size: int) -> bool:
        """Update index file in S3"""
        try:
            # Determine index key and updated_at
            year = self.year
            current_time = datetime.now()
            
            if file_type == 'metadata':
                index_key = f"metadata/tar/year={year}/court={court_code}/bench={bench}/metadata.index.json"
            else:
                index_key = f"data/tar/year={year}/court={court_code}/bench={bench}/data.index.json"
            
            # Create index data
            index_data = {
                "files": sorted([os.path.basename(f) for f in files]),
                "file_count": len(files),
                "tar_size": tar_size,
                "tar_size_human": self._format_size(tar_size),
                "updated_at": current_time.isoformat()
            }
            
            # Upload index file
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=index_key,
                Body=json.dumps(index_data, indent=2),
                ContentType='application/json'
            )
            
            print(f"  Updated index: {index_key}")
            return True
            
        except Exception as e:
            print(f"  Error updating index file: {e}")
            return False
    
    def rebuild_bench_tars(self, court_code: str, bench: str) -> bool:
        """
        Rebuild TAR files for a specific court/bench combination
        
        Args:
            court_code: Court code (e.g., "27_1")
            bench: Bench name (e.g., "hcbgoa")
            
        Returns:
            True if successful, False otherwise
        """
        print(f"\n{'='*80}")
        print(f"Processing: Court {court_code}, Bench {bench}")
        print(f"{'='*80}")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # === Process Metadata (JSON files) ===
            print("\n[1/2] Processing metadata files...")
            metadata_prefix = f"metadata/json/year={self.year}/court={court_code}/bench={bench}/"
            metadata_s3_files = self._list_s3_files(metadata_prefix, '.json')
            
            print(f"  Found {len(metadata_s3_files)} JSON files in S3")
            
            if metadata_s3_files:
                # Download metadata files
                metadata_local_dir = temp_path / "metadata"
                metadata_local_dir.mkdir(parents=True, exist_ok=True)
                
                metadata_local_files = []
                print(f"  Downloading metadata files...")
                for s3_key in tqdm(metadata_s3_files, desc="  Downloading JSON", unit="file"):
                    filename = os.path.basename(s3_key)
                    local_path = metadata_local_dir / filename
                    if self._download_file(s3_key, str(local_path)):
                        metadata_local_files.append(str(local_path))
                
                print(f"  Downloaded {len(metadata_local_files)} metadata files")
                
                # Create metadata TAR
                metadata_tar_path, metadata_tar_size = self._create_tar_from_files(
                    metadata_local_files,
                    str(temp_path),
                    "metadata.tar.gz",
                    compress=True
                )
                
                # Delete old metadata TAR
                old_metadata_tar_key = f"metadata/tar/year={self.year}/court={court_code}/bench={bench}/metadata.tar.gz"
                self._delete_s3_file(old_metadata_tar_key)
                
                # Upload new metadata TAR
                new_metadata_tar_key = f"metadata/tar/year={self.year}/court={court_code}/bench={bench}/metadata.tar.gz"
                if self._upload_file_to_s3(metadata_tar_path, new_metadata_tar_key, 'application/gzip'):
                    print(f"  ✓ Metadata TAR rebuilt successfully")
                else:
                    print(f"  ✗ Failed to upload metadata TAR")
                    return False
            else:
                print(f"  No metadata files found, skipping")
            
            # === Process Data (PDF files) ===
            print("\n[2/2] Processing data files...")
            data_prefix = f"data/pdf/year={self.year}/court={court_code}/bench={bench}/"
            data_s3_files = self._list_s3_files(data_prefix, '.pdf')
            
            print(f"  Found {len(data_s3_files)} PDF files in S3")
            
            if data_s3_files:
                # Download PDF files
                data_local_dir = temp_path / "data"
                data_local_dir.mkdir(parents=True, exist_ok=True)
                
                data_local_files = []
                print(f"  Downloading PDF files...")
                for s3_key in tqdm(data_s3_files, desc="  Downloading PDFs", unit="file"):
                    filename = os.path.basename(s3_key)
                    local_path = data_local_dir / filename
                    if self._download_file(s3_key, str(local_path)):
                        data_local_files.append(str(local_path))
                
                print(f"  Downloaded {len(data_local_files)} PDF files")
                
                # Create data TAR
                data_tar_path, data_tar_size = self._create_tar_from_files(
                    data_local_files,
                    str(temp_path),
                    "pdfs.tar",
                    compress=False
                )
                
                # Delete old data TAR
                old_data_tar_key = f"data/tar/year={self.year}/court={court_code}/bench={bench}/pdfs.tar"
                self._delete_s3_file(old_data_tar_key)
                
                # Upload new data TAR
                new_data_tar_key = f"data/tar/year={self.year}/court={court_code}/bench={bench}/pdfs.tar"
                if self._upload_file_to_s3(data_tar_path, new_data_tar_key, 'application/x-tar'):
                    print(f"  ✓ Data TAR rebuilt successfully")
                else:
                    print(f"  ✗ Failed to upload data TAR")
                    return False
            else:
                print(f"  No PDF files found, skipping")
        
        print(f"\n✓ Successfully rebuilt TARs for {court_code}/{bench}")
        return True
    
    def discover_court_bench_combinations(self) -> List[Tuple[str, str]]:
        """Discover all court/bench combinations from S3 structure"""
        print("Discovering court/bench combinations from S3...")
        
        combinations = set()
        
        # Scan both metadata and data prefixes
        prefixes = [
            f"metadata/json/year={self.year}/",
            f"data/pdf/year={self.year}/"
        ]
        
        for prefix in prefixes:
            try:
                # List all objects to find unique court/bench combinations
                paginator = self.s3_unsigned.get_paginator('list_objects_v2')
                
                for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            key = obj['Key']
                            # Extract court and bench from path like:
                            # metadata/json/year=2025/court=27_1/bench=hcbgoa/file.json
                            parts = key.split('/')
                            court_code = None
                            bench = None
                            
                            for part in parts:
                                if part.startswith('court='):
                                    court_code = part.split('=')[1]
                                elif part.startswith('bench='):
                                    bench = part.split('=')[1]
                            
                            if court_code and bench:
                                combinations.add((court_code, bench))
                
            except Exception as e:
                print(f"Error scanning {prefix}: {e}")
                continue
        
        sorted_combinations = sorted(list(combinations))
        print(f"Found {len(sorted_combinations)} court/bench combinations")
        
        return sorted_combinations
    
    def rebuild_all(self, court_filter: str = None, bench_filter: str = None) -> bool:
        """
        Rebuild all TAR files
        
        Args:
            court_filter: Optional court code to filter (e.g., "27_1")
            bench_filter: Optional bench name to filter (e.g., "hcbgoa")
        """
        combinations = self.discover_court_bench_combinations()
        
        # Apply filters
        if court_filter:
            combinations = [(c, b) for c, b in combinations if c == court_filter]
            print(f"Filtered to court {court_filter}: {len(combinations)} combinations")
        
        if bench_filter:
            combinations = [(c, b) for c, b in combinations if b == bench_filter]
            print(f"Filtered to bench {bench_filter}: {len(combinations)} combinations")
        
        if not combinations:
            print("No combinations to process")
            return True
        
        print(f"\nWill process {len(combinations)} court/bench combinations")
        
        success_count = 0
        error_count = 0
        
        for court_code, bench in combinations:
            try:
                if self.rebuild_bench_tars(court_code, bench):
                    success_count += 1
                else:
                    error_count += 1
            except Exception as e:
                print(f"Error processing {court_code}/{bench}: {e}")
                import traceback
                traceback.print_exc()
                error_count += 1
        
        print(f"\n{'='*80}")
        print(f"SUMMARY")
        print(f"{'='*80}")
        print(f"Successfully processed: {success_count} combinations")
        print(f"Errors: {error_count} combinations")
        
        return error_count == 0


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Rebuild TAR files from individual S3 files, removing duplicates"
    )
    parser.add_argument(
        '--court',
        type=str,
        help='Process only specific court (e.g., 27_1)',
        default=None
    )
    parser.add_argument(
        '--bench',
        type=str,
        help='Process only specific bench (e.g., hcbgoa)',
        default=None
    )
    parser.add_argument(
        '--year',
        type=str,
        help='Year to process (default: current year)',
        default=None
    )
    
    args = parser.parse_args()
    
    try:
        rebuilder = TARRebuilder(
            bucket_name='indian-high-court-judgments-test',
            year=args.year
        )
        
        success = rebuilder.rebuild_all(
            court_filter=args.court,
            bench_filter=args.bench
        )
        
        if success:
            print("\n✓ All TAR files rebuilt successfully!")
        else:
            print("\n✗ Some errors occurred during rebuild")
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
