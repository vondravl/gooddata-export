"""Configuration for GoodData Export.

This module handles configuration for the export functionality.
Unlike the dictionary app, this has no Flask dependencies.
"""

from os import getenv
from dotenv import load_dotenv
from typing import Optional, List


class ExportConfig:
    """Configuration for GoodData metadata export."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        workspace_id: Optional[str] = None,
        bearer_token: Optional[str] = None,
        include_child_workspaces: Optional[bool] = None,
        child_workspace_data_types: Optional[List[str]] = None,
        max_parallel_workspaces: Optional[int] = None,
        enable_rich_text_extraction: Optional[bool] = None,
        enable_post_export: Optional[bool] = None,
        debug_workspace_processing: Optional[bool] = None,
        load_from_env: bool = True,
    ):
        """Initialize export configuration.

        Args:
            base_url: GoodData API base URL
            workspace_id: GoodData workspace ID
            bearer_token: API authentication token
            include_child_workspaces: Whether to process child workspaces
            child_workspace_data_types: List of data types to fetch from child workspaces
            max_parallel_workspaces: Number of workspaces to process in parallel
            enable_rich_text_extraction: Whether to extract from rich text widgets
            enable_post_export: Whether to run post-export enrichment/procedures
            debug_workspace_processing: Enable debug logging
            load_from_env: Whether to load config from .env files
        """
        if load_from_env:
            load_dotenv(".env", override=True, interpolate=True)
            load_dotenv(".env.gdcloud", override=True, interpolate=True)

        # GoodData Config - use provided values or fall back to environment
        self.BASE_URL = base_url or getenv("BASE_URL")
        self._WORKSPACE_ID = workspace_id or getenv("WORKSPACE_ID")
        self.BEARER_TOKEN = bearer_token or getenv("BEARER_TOKEN")

        # Child workspace processing
        if include_child_workspaces is not None:
            self.INCLUDE_CHILD_WORKSPACES = include_child_workspaces
        else:
            child_workspaces_value = getenv("INCLUDE_CHILD_WORKSPACES", "False").lower()
            self.INCLUDE_CHILD_WORKSPACES = child_workspaces_value in (
                "true",
                "1",
                "yes",
                "on",
            )

        # Debug logging flag
        if debug_workspace_processing is not None:
            self.DEBUG_WORKSPACE_PROCESSING = debug_workspace_processing
        else:
            debug_env = getenv("DEBUG", "False")
            self.DEBUG_WORKSPACE_PROCESSING = debug_env.lower() in (
                "true",
                "1",
                "yes",
                "on",
            )

        # Feature flag: enable/disable rich text extraction
        # Check if explicitly set in environment
        env_rich_text_value = getenv("ENABLE_RICH_TEXT_EXTRACTION")

        if enable_rich_text_extraction is not None:
            # Explicitly set via parameter
            self._enable_rich_text_extraction = enable_rich_text_extraction
            self._rich_text_explicit = True
        elif env_rich_text_value is not None:
            # Explicitly set in .env file
            self._enable_rich_text_extraction = env_rich_text_value.lower() in (
                "true",
                "1",
                "yes",
                "on",
            )
            self._rich_text_explicit = True
        else:
            # Not explicitly set - use default (True)
            self._enable_rich_text_extraction = True
            self._rich_text_explicit = False

        # Child workspace data selection
        if child_workspace_data_types is not None:
            self.CHILD_WORKSPACE_DATA_TYPES = child_workspace_data_types
        else:
            child_data_types = getenv("CHILD_WORKSPACE_DATA_TYPES")
            if child_data_types:
                self.CHILD_WORKSPACE_DATA_TYPES = [
                    dt.strip().lower()
                    for dt in child_data_types.split(",")
                    if dt.strip()
                ]
            else:
                self.CHILD_WORKSPACE_DATA_TYPES = []

        # Maximum number of parallel workers for child workspace processing
        if max_parallel_workspaces is not None:
            self.MAX_PARALLEL_WORKSPACES = max_parallel_workspaces
        else:
            self.MAX_PARALLEL_WORKSPACES = int(getenv("MAX_WORKERS", "5"))

        # Post-export processing (enrichment/procedures)
        if enable_post_export is not None:
            self.ENABLE_POST_EXPORT = enable_post_export
        else:
            post_export_value = getenv("ENABLE_POST_EXPORT", "true").lower()
            self.ENABLE_POST_EXPORT = post_export_value in ("true", "1", "yes", "on")

        # Dynamic workspace ID
        self._workspace_id = self._WORKSPACE_ID
        self._include_child_workspaces = self.INCLUDE_CHILD_WORKSPACES

    @property
    def WORKSPACE_ID(self):
        """Get current workspace ID."""
        return getattr(self, "_workspace_id", self._WORKSPACE_ID)

    @WORKSPACE_ID.setter
    def WORKSPACE_ID(self, value):
        """Set workspace ID."""
        self._workspace_id = value

    @property
    def INCLUDE_CHILD_WORKSPACES(self):
        return getattr(self, "_include_child_workspaces", False)

    @INCLUDE_CHILD_WORKSPACES.setter
    def INCLUDE_CHILD_WORKSPACES(self, value):
        self._include_child_workspaces = bool(value)

    @property
    def ENABLE_RICH_TEXT_EXTRACTION(self):
        base_value = getattr(self, "_enable_rich_text_extraction", True)
        is_explicit = getattr(self, "_rich_text_explicit", False)

        # If explicitly set in .env or via parameter, respect that value
        if is_explicit:
            return base_value

        # Always return base_value (default True)
        # When child workspaces are included, filtering happens in export_dashboard_metrics
        return base_value

    @ENABLE_RICH_TEXT_EXTRACTION.setter
    def ENABLE_RICH_TEXT_EXTRACTION(self, value):
        self._enable_rich_text_extraction = bool(value)
        self._rich_text_explicit = True
