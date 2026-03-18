"""Base exception for bugtracker adapters."""


class BugtrackerError(Exception):
    """Raised when a bugtracker API call fails.

    All adapter-specific exceptions (e.g. ``LinearAPIError``) should
    subclass this so callers can catch a single type.
    """
