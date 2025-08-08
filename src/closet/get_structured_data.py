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
    """Loads the enriched data into a Polars DataFrame.

    Returns
    -------
    Optional[pl.DataFrame]
        A DataFrame of enriched records, or None if the file doesn't exist.
    """
    if ENRICHED_PLAYLIST_JSON_PATH.exists():
        enriched_data = pl.read_ndjson(ENRICHED_PLAYLIST_JSON_PATH)
        console.log(f"Found {len(enriched_data)} enriched records.")
        return enriched_data
    return None


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

    with console.status("Enriching records...") as status:
        for record in playlist_data:
            if record["id"] in enriched_ids:
                # Check if the record is fully enriched
                if enriched_df is not None:
                    enriched_record = enriched_df.filter(
                        pl.col("id") == record["id"]
                    ).to_dicts()
                    if (
                        enriched_record
                        and enriched_record[0].get("guest")
                        and enriched_record[0].get("movies")
                    ):
                        status.console.log(
                            f"Skipping {record['id']}: already enriched."
                        )
                        continue

            status.update(f"Enriching {record['id']}...")
            enriched_record = _extract_structured_data_from_record(record)
            srsly.write_jsonl(
                ENRICHED_PLAYLIST_JSON_PATH, [enriched_record], append=True
            )
            enriched_ids.add(record["id"])

    console.log("Enriched playlist data has been created.")


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
