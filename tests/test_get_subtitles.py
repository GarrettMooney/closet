from unittest.mock import patch

from closet.get_subtitles import get_videos_to_process


def test_get_videos_to_process(test_data_dir):
    """
    Tests that the get_videos_to_process function correctly identifies videos to process.
    """
    playlist_json_path = test_data_dir / "playlist.json"
    playlist_with_subtitles_json_path = test_data_dir / "playlist_with_subtitles.json"

    with patch("closet.get_subtitles.PLAYLIST_JSON_PATH", playlist_json_path), patch(
        "closet.get_subtitles.PLAYLIST_WITH_SUBTITLES_JSON_PATH",
        playlist_with_subtitles_json_path,
    ):
        videos_to_process, _ = get_videos_to_process()
        assert set(videos_to_process["id"].to_list()) == {"2", "4"}
