from pathlib import Path
import subprocess

from process_metadata import MetadataProcessor
from concurrent.futures import ProcessPoolExecutor

root_dir = Path("../data/aws_data")
src_dir = root_dir / "metadata/json"
pq_dir = root_dir / "metadata/parquet"


def process_court_dir(bench_dir):
    print(f"Processing {bench_dir}")
    pq_path = bench_dir.relative_to(src_dir)
    output_path = pq_dir / pq_path / "metadata.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not output_path.exists():
        mp = MetadataProcessor(bench_dir, output_path=output_path)
        mp.process()
        print(f"Processed {output_path}")

    # tar the json files
    tar_json_path = root_dir / "metadata" / "tar" / pq_path / "metadata.tar.gz"

    tar_json_path.parent.mkdir(parents=True, exist_ok=True)
    if not tar_json_path.exists():
        # use tar cli to tar the json files
        subprocess.run(["tar", "-czf", tar_json_path, "-C", bench_dir, "."])
        print(f"Tarred {tar_json_path} from {bench_dir}")
    bench_pdf_dir = root_dir / "data" / "pdf" / pq_path
    tar_pdf_path = root_dir / "data" / "tar" / pq_path / "pdfs.tar"
    tar_pdf_path.parent.mkdir(parents=True, exist_ok=True)
    if not tar_pdf_path.exists():
        subprocess.run(["tar", "-czf", tar_pdf_path, "-C", bench_pdf_dir, "."])
        print(f"Tarred {tar_pdf_path} from {bench_pdf_dir}")


def main():
    with ProcessPoolExecutor(max_workers=10) as executor:
        for year_dir in src_dir.iterdir():
            if not year_dir.is_dir():
                continue
            for court_dir in year_dir.iterdir():
                if not court_dir.is_dir():
                    continue
                for bench_dir in court_dir.iterdir():
                    if not bench_dir.is_dir():
                        continue
                    executor.submit(process_court_dir, bench_dir)


if __name__ == "__main__":
    main()
