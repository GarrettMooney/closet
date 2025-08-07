from unittest.mock import patch

import polars as pl
from closet.recommend import RankingStrategy, calculate_recommendations, load_data


def test_calculate_recommendations(enriched_playlist_path):
    """
    Tests that the calculate_recommendations function returns the expected recommendations.
    """
    with patch("closet.recommend.ENRICHED_PLAYLIST_JSON_PATH", enriched_playlist_path):
        df = load_data()
        recommendations = calculate_recommendations(
            df, ["movie a"], RankingStrategy.LIFT_SUM
        )
        assert sorted(recommendations["movie_right"].to_list()) == ["movie b", "movie c"]
