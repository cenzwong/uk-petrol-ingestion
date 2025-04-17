import json
from datetime import datetime

import boto3
import requests

company = "moto"
url = "https://moto-way.com/fuel-price/fuel_prices.json"

if __name__ == "__main__":
    print(f"Downloading data from {company}...")
    # URL to download
    response = requests.get(url)

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
