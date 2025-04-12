# small python script that goes through all the files in the current directory, even the nested fiels.
# find averge file size for .pdf and .json files

from pathlib import Path
from tqdm import tqdm

def get_file_size(file_path):
    return Path(file_path).stat().st_size


def get_average_file_size(file_path):
    return get_file_size(file_path) / len(file_path)


def get_average_file_size_for_pdf_and_json(file_path):
    pdf_files = Path(file_path).glob("**/*.pdf")
    json_files = Path(file_path).glob("**/*.json")
    pdf_size = 0
    pdf_count = 0
    json_size = 0
    json_count = 0
    for pdf_file in tqdm(pdf_files):
        pdf_size += get_file_size(pdf_file)
        pdf_count += 1
    for json_file in tqdm(json_files):
        json_size += get_file_size(json_file)
        json_count += 1

    pdf_avg_size = pdf_size / pdf_count
    json_avg_size = json_size / json_count
    print(f"PDF count: {pdf_count}, JSON count: {json_count}")
    return pdf_avg_size, json_avg_size


print(get_average_file_size_for_pdf_and_json("./data"))
