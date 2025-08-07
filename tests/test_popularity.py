from collections import Counter
from unittest.mock import patch

from closet.popularity import calculate_popularity, load_data


def test_calculate_popularity(enriched_playlist_path):
    """
    Tests that the calculate_popularity function correctly counts movie occurrences.
    """
    with patch("closet.popularity.ENRICHED_PLAYLIST_JSON_PATH", enriched_playlist_path):
        enriched_data = list(load_data())
        film_counter = calculate_popularity(enriched_data)
        expected_counter = Counter(
            {"Movie A": 2, "Movie B": 1, "Movie C": 1, "Movie D": 1}
        )
        assert film_counter == expected_counter
