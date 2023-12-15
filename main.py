import pandas as pd
import requests
from sqlalchemy import create_engine


def extract() -> dict:
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(API_URL).json()
    return data


def transform(data: dict) -> pd.DataFrame:
    df = pd.DataFrame(data)
    print(f"Total number of univerisities -> {len(data)}")
    df = df[df["name"].str.contains("California")]
    print(f"Total number of univerisities in California -> {len(df)}")
    df["domains"] = [",".join(map(str, l)) for l in df["domains"]]
    df["web_pages"] = [",".join(map(str, l)) for l in df["web_pages"]]
    df = df.reset_index(drop=True)
    return df[["name", "domains", "web_pages"]]


def load(df: pd.DataFrame) -> None:
    disk_engine = create_engine("sqlite:///store.db")
    df.to_sql("cali_uni", disk_engine, if_exists="replace")


if __name__ == "__main__":
    data = extract()
    df = transform(data)
    load(df)
