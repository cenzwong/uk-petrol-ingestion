import json
from datetime import datetime

import boto3
import requests

company = "jet_retail_uk"
url = "https://jetlocal.co.uk/fuel_prices_data.json"

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-GB,en;q=0.9",
    "Priority": "u=0, i",
    "Sec-CH-UA": '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
}

if __name__ == "__main__":
    print(f"Downloading data from {company}...")
    # URL to download
    response = requests.get(url, headers=headers)

    fuel_prices_data_json: dict = response.json()

    last_updated_str = fuel_prices_data_json["last_updated"]
    print(f"{last_updated_str=}")

    last_updated_path = datetime.strptime(
        last_updated_str, "%d/%m/%Y %H:%M:%S"
    ).strftime("%Y%m%d%H%M")

    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket="uk-fuel-price-data",
        Key=f"landing/{company}/{last_updated_path}/fuel_prices_data.json",
        Body=json.dumps(fuel_prices_data_json),
    )

    print("done")
