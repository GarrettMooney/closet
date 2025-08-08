from unittest.mock import patch

import polars as pl
from closet.recommend import (
    calculate_co_occurrence,
    calculate_lift,
    calculate_movie_popularity,
    calculate_recommendations,
    get_movie_pairs,
    load_data,
)


def test_calculate_recommendations(enriched_playlist_path):
    """
    Tests that the calculate_recommendations function returns the expected recommendations.
    """
    with patch("closet.recommend.ENRICHED_PLAYLIST_JSON_PATH", enriched_playlist_path):
        df = load_data()
        recommendations = calculate_recommendations(df, ["movie a"])
        assert sorted(recommendations["movie_right"].to_list()) == ["movie b", "movie c"]


def test_get_movie_pairs(enriched_playlist_path):
    """
    Tests that the get_movie_pairs function returns the expected movie pairs.
    """
    with patch("closet.recommend.ENRICHED_PLAYLIST_JSON_PATH", enriched_playlist_path):
        df = load_data()
        movie_pairs = get_movie_pairs(df)
        assert len(movie_pairs) == 4
        assert sorted(movie_pairs["movie"].to_list()) == [
            "movie a",
            "movie a",
            "movie b",
            "movie c",
        ]


def test_calculate_co_occurrence(enriched_playlist_path):
    """
    Tests that the calculate_co_occurrence function returns the expected co-occurrence.
    """
    with patch("closet.recommend.ENRICHED_PLAYLIST_JSON_PATH", enriched_playlist_path):
        df = load_data()
        movie_pairs = get_movie_pairs(df)
        co_occurrence = calculate_co_occurrence(movie_pairs)
        assert len(co_occurrence) == 4


def test_calculate_movie_popularity(enriched_playlist_path):
    """
    Tests that the calculate_movie_popularity function returns the expected popularity.
    """
    with patch("closet.recommend.ENRICHED_PLAYLIST_JSON_PATH", enriched_playlist_path):
        df = load_data()
        movie_pairs = get_movie_pairs(df)
        popularity = calculate_movie_popularity(movie_pairs)
        assert len(popularity) == 3
        assert popularity.sort("movie")["popularity"].to_list() == [2, 1, 1]


def test_calculate_lift(enriched_playlist_path):
    """
    Tests that the calculate_lift function returns the expected lift.
    """
    with patch("closet.recommend.ENRICHED_PLAYLIST_JSON_PATH", enriched_playlist_path):
        df = load_data()
        movie_pairs = get_movie_pairs(df)
        co_occurrence = calculate_co_occurrence(movie_pairs)
        popularity = calculate_movie_popularity(movie_pairs)
        lift = calculate_lift(co_occurrence, popularity)
        assert len(lift) == 4
