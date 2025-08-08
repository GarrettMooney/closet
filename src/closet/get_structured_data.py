import sys
from typing import Dict, Generator, Iterable, Optional, Set

import polars as pl
import srsly

from closet.config import ENRICHED_PLAYLIST_JSON_PATH, PLAYLIST_WITH_SUBTITLES_JSON_PATH
from closet.enrich import _extract_structured_data_from_record
from closet.logging import console


def load_data() -> Generator[Dict, None, None]:
    """Loads the playlist data with subtitles.

    Yields
    ------
    Dict
        The playlist data as a generator of dictionaries.

    Raises
    ------
    FileNotFoundError
        If the playlist data file is not found.
    """
    if not PLAYLIST_WITH_SUBTITLES_JSON_PATH.exists():
        raise FileNotFoundError("No playlist with subtitles found.")
    yield from srsly.read_jsonl(PLAYLIST_WITH_SUBTITLES_JSON_PATH)


def load_enriched_data() -> Optional[pl.DataFrame]:
    """Loads and deduplicates the enriched data.

    Returns
    -------
    Optional[pl.DataFrame]
        A DataFrame of enriched records, or None if the file doesn't exist.
    """
    if not ENRICHED_PLAYLIST_JSON_PATH.exists():
        return None

    enriched_data = pl.read_ndjson(ENRICHED_PLAYLIST_JSON_PATH)
    
    # Add a score to prioritize which record to keep
    enriched_data = enriched_data.with_columns(
        (pl.col("guest").is_not_null() & pl.col("movies").is_not_null()).alias("score")
    )
    
    # Deduplicate
    enriched_data = enriched_data.sort("score", descending=True).unique(
        subset=["id"], keep="first"
    )
    
    # Save the cleaned data
    cleaned_df = enriched_data.drop("score")
    cleaned_df.write_ndjson(ENRICHED_PLAYLIST_JSON_PATH)
    
    console.log(f"Found and cleaned {len(cleaned_df)} enriched records.")
    return cleaned_df


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
    enriched_ids = (
        set(enriched_df["id"].to_list()) if enriched_df is not None else set()
    )
    newly_enriched_records = []

    with console.status("Enriching records...") as status:
        for record in playlist_data:
            if record["id"] in enriched_ids:
                if enriched_df is not None:
                    existing_record = enriched_df.filter(
                        pl.col("id") == record["id"]
                    ).to_dicts()
                    if (
                        existing_record
                        and existing_record[0].get("guest")
                        and existing_record[0].get("movies")
                    ):
                        status.console.log(
                            f"Skipping {record['id']}: already enriched."
                        )
                        continue

            status.update(f"Enriching {record['id']}...")
            enriched_record = _extract_structured_data_from_record(record)
            newly_enriched_records.append(enriched_record)
            enriched_ids.add(record["id"])

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
    playlist_data = load_data()
    enriched_df = load_enriched_data()
    enrich_data(playlist_data, enriched_df)


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        console.log(str(e), style="red")
        sys.exit(1)
