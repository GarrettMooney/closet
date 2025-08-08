from unittest.mock import patch

from closet.main import main


@patch("closet.main.get_new_videos")
@patch("closet.main.get_subtitles")
@patch("closet.main.get_structured_data")
def test_main(mock_get_structured_data, mock_get_subtitles, mock_get_new_videos):
    """Test that the main function calls the pipeline steps in the correct order."""
    # Act
    main()

    # Assert
    mock_get_new_videos.assert_called_once()
    mock_get_subtitles.assert_called_once()
    mock_get_structured_data.assert_called_once()
