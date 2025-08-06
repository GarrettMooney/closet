class SubtitleError(Exception):
    """Base exception for subtitle-related errors."""
    pass


class RateLimitError(SubtitleError):
    """Exception raised for 429 rate-limiting errors."""
    pass
