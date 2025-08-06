import json
import random
import subprocess
import time
from pathlib import Path

import polars as pl
import srsly

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
    data = [srsly.json_loads(line) for line in result.stdout.strip().split("\n")]
    srsly.write_json(PLAYLIST_JSON_PATH, data, indent=2)


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


from closet.errors import RateLimitError, SubtitleError
from closet.logging import console
from closet.subtitles import get_subtitles

PLAYLIST_WITH_SUBTITLES_JSON_PATH = DATA_DIR / "playlist_with_subtitles.json"
LOG_HTML_PATH = DATA_DIR / "log.html"


def main():
    """
    Orchestrates the fetching of playlist data and subtitles.
    This function is designed to be resumable and idempotent.
    """
    # Step 1: Fetch the latest playlist data
    fetch_playlist(YOUTUBE_PLAYLIST_URL)

    # Step 2: Load the playlist data and select the relevant columns
    playlist_data = srsly.read_json(PLAYLIST_JSON_PATH)
    playlist_df = pl.DataFrame(playlist_data).select(["id", "url", "title"])

    # Step 3: Check for existing enriched data
    if PLAYLIST_WITH_SUBTITLES_JSON_PATH.exists():
        try:
            enriched_data = list(srsly.read_jsonl(PLAYLIST_WITH_SUBTITLES_JSON_PATH))
            enriched_df = pl.DataFrame(enriched_data).select(
                ["id", "url", "title", "subtitles"]
            )
            # Filter out videos that already have subtitles
            playlist_df = playlist_df.filter(
                ~pl.col("id").is_in(enriched_df["id"])
            )
        except Exception:
            # If the existing file is malformed, start fresh
            enriched_df = pl.DataFrame()
    else:
        enriched_df = pl.DataFrame()

    # Step 4: Process videos that are missing subtitles
    if not playlist_df.is_empty():
        subtitles = []
        for video_id in playlist_df["id"]:
            retries = 5
            base_delay = 1.0
            for attempt in range(retries):
                try:
                    subtitles.append(get_subtitles(video_id))
                    console.log(
                        f"Successfully fetched subtitles for {video_id}", style="green"
                    )
                    break  # Success
                except RateLimitError as e:
                    delay = base_delay * (2**attempt) + random.uniform(0, 1)
                    console.log(f"{e}. Retrying in {delay:.2f} seconds...", style="yellow")
                    time.sleep(delay)
                except SubtitleError as e:
                    console.log(
                        f"Could not fetch subtitles for {video_id}: {e}", style="red"
                    )
                    subtitles.append(None)
                    break  # Non-retriable error
                except Exception as e:
                    console.log(
                        f"An unexpected error occurred for {video_id}: {e}", style="red"
                    )
                    subtitles.append(None)
                    break  # Non-retriable error
            else:
                # All retries failed
                console.log(f"All retries failed for {video_id}.", style="red")
                subtitles.append(None)
        
        playlist_df = playlist_df.with_columns(pl.Series("subtitles", subtitles))
        
        # Step 5: Combine with existing enriched data
        if not enriched_df.is_empty():
            enriched_df = pl.concat([enriched_df, playlist_df])
        else:
            enriched_df = playlist_df

        # Step 6: Save the updated enriched data with the correct schema
        data_to_save = (
            enriched_df.select(["id", "url", "title", "subtitles"])
            .to_dicts()
        )
        srsly.write_jsonl(PLAYLIST_WITH_SUBTITLES_JSON_PATH, data_to_save)
        console.log("Playlist with subtitles has been updated.")
    else:
        console.log("No new videos to process.")

    console.save_html(str(LOG_HTML_PATH))


if __name__ == "__main__":
    main()
