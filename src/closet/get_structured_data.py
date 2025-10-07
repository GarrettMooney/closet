import sys
from itertools import filterfalse
from typing import Dict, Generator, Iterable, Optional, Set

import polars as pl
import srsly

from closet.config import ENRICHED_PLAYLIST_JSON_PATH, PLAYLIST_WITH_SUBTITLES_JSON_PATH
from closet.enrich import _extract_structured_data_from_record
from closet.logging import console


def load_playlist_with_subtitles() -> Generator[Dict, None, None]:
    """Loads the playlist data with subtitles.

    Yields
    ------
    Generator[Dict, None, None]
        A generator of dictionaries, where each dictionary represents a video.

    Raises
    ------
    FileNotFoundError
        If the playlist data file is not found.
    """
    if not PLAYLIST_WITH_SUBTITLES_JSON_PATH.exists():
        raise FileNotFoundError("No playlist with subtitles found.")
    yield from srsly.read_jsonl(PLAYLIST_WITH_SUBTITLES_JSON_PATH)


def load_enriched_data() -> Optional[pl.DataFrame]:
    """Loads the enriched data from the JSON file.

    Returns
    -------
    Optional[pl.DataFrame]
        A DataFrame of enriched records, or None if the file doesn't exist.
    """
    if not ENRICHED_PLAYLIST_JSON_PATH.exists():
        return None
    return pl.read_ndjson(ENRICHED_PLAYLIST_JSON_PATH)


def clean_enriched_data(enriched_df: pl.DataFrame) -> pl.DataFrame:
    """Deduplicates and cleans the enriched data.

    Parameters
    ----------
    enriched_df : pl.DataFrame
        The DataFrame of enriched records.

    Returns
    -------
    pl.DataFrame
        A cleaned DataFrame with duplicates removed.
    """
    enriched_df = enriched_df.with_columns(
        (pl.col("guest").is_not_null() & pl.col("movies").is_not_null()).alias("score")
    )
    cleaned_df = enriched_df.sort("score", descending=True).unique(
        subset=["id"], keep="first"
    )
    return cleaned_df.drop("score")


def enrich_data(
    playlist_data: Iterable[Dict], enriched_df: Optional[pl.DataFrame]
) -> None:
    """Enriches the playlist data with structured information.

    Parameters
    ----------
    playlist_data : Iterable[Dict]
        An iterator of the playlist data to enrich.
    enriched_df : Optional[pl.DataFrame]
        A DataFrame of already enriched records.
    """
    enriched_ids: Set[str] = (
        set(enriched_df["id"].to_list()) if enriched_df is not None else set()
    )

    records_to_enrich = filterfalse(
        lambda record: record["id"] in enriched_ids, playlist_data
    )

    newly_enriched_records = []
    with console.status("Enriching records...") as status:
        for record in records_to_enrich:
            status.update(f"Enriching {record['id']}...")
            enriched_record = _extract_structured_data_from_record(record)
            newly_enriched_records.append(enriched_record)

    if newly_enriched_records:
        new_df = pl.from_dicts(newly_enriched_records)
        if enriched_df is not None:
            combined_df = pl.concat([enriched_df, new_df])
        else:
            combined_df = new_df

        combined_df.unique(subset=["id"], keep="last").write_ndjson(
            ENRICHED_PLAYLIST_JSON_PATH
        )
        console.log(f"Enriched and saved {len(newly_enriched_records)} new records.")
    else:
        console.log("No new records to enrich.")


def main():
    """Main entry point for the script."""
    try:
        playlist_data = load_playlist_with_subtitles()
        enriched_df = load_enriched_data()
        if enriched_df is not None:
            enriched_df = clean_enriched_data(enriched_df)
        enrich_data(playlist_data, enriched_df)
    except FileNotFoundError as e:
        console.log(str(e), style="red")
        sys.exit(1)


if __name__ == "__main__":
    main()
