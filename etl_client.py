from io import StringIO
import os
import httpx
import pandas as pd
import asyncio
from datetime import datetime, timedelta
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt

APP_BASE_URL = "http://localhost:8000"
API_KEY = "ADU8S67Ddy!d7f?"

class Endpoint(BaseModel):
    name: str
    path: str
    content_type: str


@retry(stop=stop_after_attempt(7))
async def extract(client: httpx.AsyncClient, endpoint: Endpoint, requested_date: str) -> StringIO:
    path = f"/{requested_date}/renewables/{endpoint.path}?api_key={API_KEY}"
    resp = await client.get(path)
    resp.raise_for_status()
    return StringIO(resp.content.decode())


def transform(data: StringIO, endpoint: Endpoint) -> pd.DataFrame:
    if endpoint.content_type == "application/json":
        df = _transform_json(data)
    elif endpoint.content_type == "text/csv":
        df = _transform_csv(data)
    else:
        raise ValueError(f"Unsupported content_type: {endpoint.content_type}")
    
    df = df.rename(columns={
        "Naive_Timestamp ": "naive_timestamp", 
        " Variable": "variable", 
        "value": "value",
        "Last Modified utc": "last_modified_utc",
    })
    return df


def _transform_json(data: StringIO) -> pd.DataFrame:
    df = pd.read_json(data)
    df["Naive_Timestamp "] = pd.to_datetime(df["Naive_Timestamp "], utc=True, unit='ms')
    return df


def _transform_csv(data: StringIO) -> pd.DataFrame:
    df = pd.read_csv(data)
    return df


def _create_dirs(output_dir: str):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)


def load(endpoint: Endpoint, df: pd.DataFrame, output_dir: str, requested_date: str) -> None:
    folder_path = os.path.join(output_dir, f"api={endpoint.name}", f"requested_date={requested_date}")
    _create_dirs(folder_path)
    output_path = os.path.join(folder_path, f'{endpoint.name}.csv')
    df.to_csv(output_path, index=False)


async def main():
    solar = Endpoint(name="solar", path="solargen.json", content_type="application/json")
    wind = Endpoint(name="wind", path="windgen.csv", content_type="text/csv")

    today = datetime.today()
    start_of_week = (today - timedelta(days=7)).date()
    date_range = [start_of_week + timedelta(days=i) for i in range(7)]

    async with httpx.AsyncClient(base_url=APP_BASE_URL) as client:
        for requested_date in date_range:
            for endpoint in [solar, wind]:
                data = await extract(client, endpoint, str(requested_date))
                df = transform(data, endpoint)
                load(endpoint, df, './output', requested_date)


if __name__ == "__main__":
    asyncio.run(main())
