import random
import time
from typing import Tuple

import polars as pl
import srsly

from closet.config import PLAYLIST_JSON_PATH, PLAYLIST_WITH_SUBTITLES_JSON_PATH
from closet.errors import RateLimitError, SubtitleError
from closet.logging import console
from closet.subtitles import get_subtitles


def get_videos_to_process() -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Identifies which videos need to be processed for subtitles.

    Returns
    -------
    Tuple[pl.DataFrame, pl.DataFrame]
        A tuple containing two DataFrames:
        - The first DataFrame contains the videos that need to be processed.
        - The second DataFrame contains the videos that are already enriched with subtitles.
    """
    playlist_data = srsly.read_json(PLAYLIST_JSON_PATH)
    playlist_df = pl.DataFrame(playlist_data).select(["id", "url", "title"])

    if not PLAYLIST_WITH_SUBTITLES_JSON_PATH.exists():
        return playlist_df, pl.DataFrame()

    try:
        enriched_data = list(srsly.read_jsonl(PLAYLIST_WITH_SUBTITLES_JSON_PATH))
        enriched_df = pl.DataFrame(enriched_data).select(
            ["id", "url", "title", "subtitles"]
        )

        new_videos_df = playlist_df.join(
            enriched_df, on="id", how="anti"
        )
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
    except Exception:
        return playlist_df, pl.DataFrame()


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
    for video_id in videos_df["id"]:
        retries = 3
        base_delay = 1.0
        for attempt in range(retries):
            try:
                subtitles.append(get_subtitles(video_id))
                console.log(
                    f"Successfully fetched subtitles for {video_id}", style="green"
                )
                break
            except RateLimitError as e:
                delay = base_delay * (2**attempt) + random.uniform(0, 1)
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
        time.sleep(1)

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
    if not existing_enriched_df.is_empty():
        final_df = pl.concat([existing_enriched_df, newly_enriched_df])
    else:
        final_df = newly_enriched_df

    data_to_save = final_df.select(["id", "url", "title", "subtitles"]).to_dicts()
    srsly.write_jsonl(PLAYLIST_WITH_SUBTITLES_JSON_PATH, data_to_save)
    console.log("Playlist with subtitles has been updated.")


def main():
    """Main entry point for the script."""
    videos_to_process_df, existing_enriched_df = get_videos_to_process()

    if videos_to_process_df.is_empty():
        console.log("No new videos or missing subtitles to process. Exiting.")
        return

    newly_enriched_df = fetch_subtitles_for_videos(videos_to_process_df)
    update_enriched_playlist(newly_enriched_df, existing_enriched_df)


if __name__ == "__main__":
    main()
