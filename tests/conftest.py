from pathlib import Path

import pytest


@pytest.fixture
def test_data_dir() -> Path:
    """Returns the path to the test data directory."""
    return Path(__file__).parent / "data"


@pytest.fixture
def enriched_playlist_path(test_data_dir: Path) -> Path:
    """Returns the path to the enriched playlist test data."""
    return test_data_dir / "enriched_playlist.json"


@pytest.fixture
def playlist_with_subtitles_path(test_data_dir: Path) -> Path:
    """Returns the path to the playlist with subtitles test data."""
    return test_data_dir / "playlist_with_subtitles.json"


@pytest.fixture
def playlist_path(test_data_dir: Path) -> Path:
    """Returns the path to the playlist test data."""
    return test_data_dir / "playlist.json"
