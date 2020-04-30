import os
import re
import requests
import zipfile


DATA_URL = "https://www.sec.gov/data.json"
DATA_PATH = "data/SECData"
ZIPPED_DATASETS = [
    "Conditional Cancel and Trade Distributions",
    "Metrics by Individual Security",
    "Mutual Fund Prospectus Risk_Return Summary Data Sets",
    "Summary Metrics by Decile and Quartile",
    "Form D Data Sets",
    "FOIA Logs",
    "Information About Registered Municipal Advisors",
    "Financial Statement and Notes Data Sets",
    "Crowdfunding Offerings Data Sets",
    "Financial Statement Data Sets",
    "Information About Registered Investment Advisers and Exempt Reporting Advisers",
    "Metrics by Individual Security and Exchange",
    "Regulation A Data Sets",
    "Fails-to-Deliver Data",
    "Spreads and Depth by Individual Security",
    "Summary Metrics by Exchange",
    "Hazards and Survivors by Time Period",
]


def download_file(file_url: str, local_fp: str):
    data = requests.get(file_url)
    with open(local_fp, "wb") as f:
        f.write(data.content)


def make_dir(path: str):
    if not os.path.exists(path):
        os.mkdir(path)


def download_sec_data(data_path: str, overwrite=True):
    # TODO parallelize this
    data_json = requests.get(DATA_URL).json()
    datasets = data_json["dataset"]
    make_dir(data_path)
    for i, dataset in enumerate(datasets):

        title = dataset["title"]
        title = re.sub("[^\w\-_\. ]", "_", title)
        print(f"Pulling dataset {i+1} of {len(datasets)}")

        dataset_path = os.path.join(data_path, title)
        make_dir(dataset_path)
        try:
            for dataset_file in dataset["distribution"]:
                name = dataset_file["title"]
                name = re.sub("[^\w\-_\. ]", "_", name)
                dataset_file_path = os.path.join(dataset_path, name)
                file_type = dataset_file["downloadURL"].split(".")[-1]
                dataset_file_path_ext = dataset_file_path + f".{file_type}"
                if not overwrite:
                    if os.path.exists(dataset_file_path_ext):
                        continue

                download_file(
                    dataset_file["downloadURL"], dataset_file_path_ext
                )
        except:
            print(f"Issue with {dataset['title']} {dataset_file['title']}")


def extract_files(zip_file_path: str):
    unzip_folder = zip_file_path[:-4]
    with zipfile.ZipFile(zip_file_path, "r") as zr:
        zr.extractall(unzip_folder)


def extract_files_in_folders(folders=ZIPPED_DATASETS):
    # TODO parallelize this
    for folder in folders:
        print(f"extracting files from {folder}")
        files = os.listdir(folder)
        for f in files:
            if ".zip" in f:
                fpath = os.path.join(folder, f)
                if not os.path.exists(fpath[:-4]):
                    try:
                        extract_files(fpath)
                    except:
                        print(f"issue with {fpath}")


if __name__ == "__main__":
    download_sec_data(DATA_PATH, overwrite=False)
    extract_files_in_folders(ZIPPED_DATASETS)
