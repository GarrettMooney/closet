import sys
from enum import Enum
from typing import List, Tuple

import polars as pl

from closet.config import ENRICHED_PLAYLIST_JSON_PATH
from closet.logging import console


def parse_args(args: List[str]) -> Tuple[str, List[str]]:
    """Parses command-line arguments.

    Parameters
    ----------
    args : List[str]
        The command-line arguments.

    Returns
    -------
    Tuple[str, List[str]]
        A tuple containing the titles string and the list of titles.
    """
    if "--help" in args or "-h" in args:
        console.log('Usage: recommend "<title1>,<title2>,..."')
        sys.exit(0)

    if len(args) < 2:
        console.log(
            'Usage: recommend "<title1>,<title2>,..."',
            style="red",
        )
        sys.exit(1)

    titles_str = args[1]
    title_list = [title.strip().lower() for title in titles_str.split(",")]

    return titles_str, title_list


def load_data() -> pl.DataFrame:
    """Loads the enriched playlist data.

    Returns
    -------
    pl.DataFrame
        The enriched playlist data as a Polars DataFrame.

    Raises
    ------
    FileNotFoundError
        If the enriched playlist data file is not found.
    """
    if not ENRICHED_PLAYLIST_JSON_PATH.exists():
        raise FileNotFoundError("Enriched playlist data not found.")
    return pl.read_ndjson(ENRICHED_PLAYLIST_JSON_PATH).drop_nulls()


def calculate_recommendations(
    df: pl.DataFrame, title_list: List[str]
) -> pl.DataFrame:
    """Calculates movie recommendations.

    Parameters
    ----------
    df : pl.DataFrame
        The enriched playlist data.
    title_list : List[str]
        The list of titles to get recommendations for.

    Returns
    -------
    pl.DataFrame
        A DataFrame with the movie recommendations.
    """
    movie_pairs = df.explode("movies").select(
        pl.col("id"),
        pl.col("movies").struct.field("title").str.to_lowercase().alias("movie"),
    )
    co_occurrence = (
        movie_pairs.join(movie_pairs, on="id")
        .filter(pl.col("movie") != pl.col("movie_right"))
        .group_by(["movie", "movie_right"])
        .agg(pl.len().alias("co_occurrence"))
    )
    movie_popularity = movie_pairs.group_by("movie").agg(pl.len().alias("popularity"))
    recommendations = (
        co_occurrence.join(movie_popularity, left_on="movie", right_on="movie")
        .join(
            movie_popularity,
            left_on="movie_right",
            right_on="movie",
            suffix="_right",
        )
        .with_columns(
            (
                pl.col("co_occurrence")
                / (pl.col("popularity") * pl.col("popularity_right"))
            ).alias("lift")
        )
    )
    filtered_recommendations = recommendations.filter(pl.col("movie").is_in(title_list))
    all_scores = filtered_recommendations.group_by("movie_right").agg(
        pl.sum("lift").alias("lift_sum"),
        (pl.col("lift") / pl.col("popularity_right")).sum().alias("weighted_average"),
        pl.len().alias("frequency"),
    )

    sort_by = ["lift_sum", "weighted_average", "frequency"]
    return all_scores.sort(sort_by, descending=True)


def display_recommendations(
    df: pl.DataFrame,
    final_recommendations: pl.DataFrame,
    titles_str: str,
):
    """Displays the final recommendations.

    Parameters
    ----------
    df : pl.DataFrame
        The original enriched playlist data.
    final_recommendations : pl.DataFrame
        The calculated recommendations.
    titles_str : str
        The original string of titles.
    """
    final_recommendations_with_desc = final_recommendations.join(
        df.explode("movies")
        .select(
            pl.col("movies")
            .struct.field("title")
            .str.to_lowercase()
            .alias("movie_right"),
            pl.col("movies").struct.field("description"),
        )
        .unique("movie_right"),
        on="movie_right",
    )

    console.log(f"Recommendations for {titles_str}:")

    if final_recommendations_with_desc.height > 0:
        top_score = (
            final_recommendations_with_desc.select(pl.col("lift_sum")).max().item()
        )
        top_recommendations = final_recommendations_with_desc.filter(
            pl.col("lift_sum") == top_score
        )

        if (
            top_recommendations.height < 10
            and final_recommendations_with_desc.height > top_recommendations.height
        ):
            recs_to_display = final_recommendations_with_desc.head(10)
        else:
            recs_to_display = top_recommendations
    else:
        recs_to_display = final_recommendations_with_desc

    for row in recs_to_display.iter_rows(named=True):
        console.log(
            f"- [green]{row['movie_right'].title()}[/green]: {row['description']}"
        )


def main():
    """Main entry point for the script."""
    titles_str, title_list = parse_args(sys.argv)
    df = load_data()
    recommendations = calculate_recommendations(df, title_list)
    display_recommendations(df, recommendations, titles_str)


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        console.log(str(e), style="red")
        sys.exit(1)
