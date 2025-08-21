import random
import time
from typing import Tuple

import polars as pl
import srsly

from closet.config import (
    PLAYLIST_JSON_PATH,
    PLAYLIST_WITH_SUBTITLES_JSON_PATH,
    COOKIES_FILE,
)
from closet.errors import RateLimitError, SubtitleError
from closet.logging import console
from closet.subtitles import get_subtitles


def load_playlist() -> pl.DataFrame:
    """Loads the playlist data from the JSON file.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the playlist data.
    """
    playlist_data = srsly.read_json(PLAYLIST_JSON_PATH)
    return pl.DataFrame(playlist_data).select(["id", "url", "title"])


def load_enriched_playlist() -> pl.DataFrame:
    """Loads the enriched playlist data from the JSONL file.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the enriched playlist data.
    """
    if not PLAYLIST_WITH_SUBTITLES_JSON_PATH.exists():
        return pl.DataFrame()
    try:
        enriched_data = list(srsly.read_jsonl(PLAYLIST_WITH_SUBTITLES_JSON_PATH))
        return pl.DataFrame(enriched_data).select(["id", "url", "title", "subtitles"])
    except Exception:
        return pl.DataFrame()


def get_videos_to_process(
    playlist_df: pl.DataFrame, enriched_df: pl.DataFrame
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Identifies which videos need to be processed for subtitles.

    Parameters
    ----------
    playlist_df : pl.DataFrame
        The DataFrame of all videos in the playlist.
    enriched_df : pl.DataFrame
        The DataFrame of videos that are already enriched with subtitles.

    Returns
    -------
    Tuple[pl.DataFrame, pl.DataFrame]
        A tuple containing two DataFrames:
        - The first DataFrame contains the videos that need to be processed.
        - The second DataFrame contains the videos that are already enriched with subtitles.
    """
    if enriched_df.is_empty():
        return playlist_df, pl.DataFrame()

    new_videos_df = playlist_df.join(enriched_df, on="id", how="anti")
    missing_subtitles_df = enriched_df.filter(pl.col("subtitles").is_null())

    videos_to_process_df = pl.concat(
        [
            new_videos_df,
            missing_subtitles_df.select(["id", "url", "title"]),
        ]
    ).unique(subset=["id"])

    return (
        videos_to_process_df,
        enriched_df.filter(pl.col("subtitles").is_not_null()),
    )


def fetch_subtitles_for_videos(videos_df: pl.DataFrame) -> pl.DataFrame:
    """Fetches subtitles for the given videos.

    Parameters
    ----------
    videos_df : pl.DataFrame
        A DataFrame of videos to process.

    Returns
    -------
    pl.DataFrame
        A DataFrame with the subtitles added.
    """
    subtitles = []
    cookies_file = (
        COOKIES_FILE if COOKIES_FILE and Path(COOKIES_FILE).exists() else None
    )

    if cookies_file:
        console.log(f"Using cookies file: {cookies_file}", style="blue")
    else:
        console.log(
            "No cookies file found. This may trigger YouTube's anti-bot detection.",
            style="yellow",
        )

    for video in videos_df.iter_rows(named=True):
        video_id = video["id"]
        retries = 3
        base_delay = 2.0  # Increased base delay
        for attempt in range(retries):
            try:
                subtitle = get_subtitles(video_id, cookies_file=cookies_file)
                subtitles.append(subtitle)
                console.log(
                    f"Successfully fetched subtitles for {video_id}", style="green"
                )
                break
            except RateLimitError as e:
                delay = base_delay * (2**attempt) + random.uniform(0, 2)
                console.log(f"{e}. Retrying in {delay:.2f} seconds...", style="yellow")
                time.sleep(delay)
            except SubtitleError as e:
                console.log(
                    f"Could not fetch subtitles for {video_id}: {e}", style="red"
                )
                subtitles.append(None)
                break
            except Exception as e:
                console.log(
                    f"An unexpected error occurred for {video_id}: {e}", style="red"
                )
                subtitles.append(None)
                break
        else:
            console.log(f"All retries failed for {video_id}.", style="red")
            subtitles.append(None)

        # Longer sleep between videos to avoid triggering rate limits
        time.sleep(random.uniform(2, 4))

    return videos_df.with_columns(pl.Series("subtitles", subtitles))


def update_enriched_playlist(
    newly_enriched_df: pl.DataFrame, existing_enriched_df: pl.DataFrame
):
    """Updates the enriched playlist with the newly fetched data.

    Parameters
    ----------
    newly_enriched_df : pl.DataFrame
        A DataFrame with the newly enriched data.
    existing_enriched_df : pl.DataFrame
        A DataFrame with the existing enriched data.
    """
    final_df = (
        pl.concat([existing_enriched_df, newly_enriched_df])
        if not existing_enriched_df.is_empty()
        else newly_enriched_df
    )
    final_df = final_df.unique(subset=["id"], keep="last")
    data_to_save = final_df.select(["id", "url", "title", "subtitles"]).to_dicts()
    srsly.write_jsonl(PLAYLIST_WITH_SUBTITLES_JSON_PATH, data_to_save, append=False)
    console.log("Playlist with subtitles has been updated.")


def main():
    """Main entry point for the script."""
    playlist_df = load_playlist()
    enriched_df = load_enriched_playlist()
    videos_to_process_df, existing_enriched_df = get_videos_to_process(
        playlist_df, enriched_df
    )

    if videos_to_process_df.is_empty():
        console.log("No new videos or missing subtitles to process. Exiting.")
        return

    newly_enriched_df = fetch_subtitles_for_videos(videos_to_process_df)
    update_enriched_playlist(newly_enriched_df, existing_enriched_df)


if __name__ == "__main__":
    main()
