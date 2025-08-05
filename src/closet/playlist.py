import json
import subprocess
from pathlib import Path

import polars as pl

YOUTUBE_PLAYLIST_URL = "https://www.youtube.com/playlist?list=PL7D89754A5DAD1E8E"
DATA_DIR = Path("data")
PLAYLIST_JSON_PATH = DATA_DIR / "playlist.json"


def fetch_playlist(playlist_url: str) -> None:
    """Fetch the playlist from YouTube and save it to a file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    command = [
        "yt-dlp",
        "--dump-json",
        "--flat-playlist",
        playlist_url,
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    with open(PLAYLIST_JSON_PATH, "w") as f:
        # The output from yt-dlp is a series of JSON objects, one per line.
        # We need to parse each line as a separate JSON object.
        data = [json.loads(line) for line in result.stdout.strip().split("\n")]
        json.dump(data, f, indent=2)


def get_playlist_length(lf: pl.LazyFrame) -> int:
    return lf.select("playlist_count").unique().collect().item()


def extract_relevant_columns(lf: pl.LazyFrame) -> pl.DataFrame:
    return lf.select(
        [
            pl.col("id"),
            pl.col("url"),
            pl.col("title"),
        ]
    ).collect()


def get_new_records(lf: pl.LazyFrame, last_id: int) -> pl.DataFrame:
    return lf.filter(pl.col("id") > pl.lit(last_id)).pipe(extract_relevant_columns)


def extract_movie_list(record):
    # Placeholder function to extract movie list from a record
    pass


def main():
    playlist_plan = pl.read_json(PLAYLIST_JSON_PATH).lazy()
    for record in playlist_plan.pipe(extract_relevant_columns).iter_rows(named=True):
        print(record)


if __name__ == "__main__":
    main()
