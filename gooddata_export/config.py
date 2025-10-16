"""Configuration for GoodData Export.

This module handles configuration for the export functionality.
Unlike the dictionary app, this has no Flask dependencies.
"""
from os import getenv
from dotenv import load_dotenv
from typing import Optional, List


class ExportConfig:
    """Configuration for GoodData metadata export."""

    def __init__(self, 
                 base_url: Optional[str] = None,
                 workspace_id: Optional[str] = None,
                 bearer_token: Optional[str] = None,
                 include_child_workspaces: bool = False,
                 child_workspace_data_types: Optional[List[str]] = None,
                 max_parallel_workspaces: int = 5,
                 enable_rich_text_extraction: bool = False,
                 debug_workspace_processing: bool = False,
                 load_from_env: bool = True):
        """Initialize export configuration.
        
        Args:
            base_url: GoodData API base URL
            workspace_id: GoodData workspace ID
            bearer_token: API authentication token
            include_child_workspaces: Whether to process child workspaces
            child_workspace_data_types: List of data types to fetch from child workspaces
            max_parallel_workspaces: Number of workspaces to process in parallel
            enable_rich_text_extraction: Whether to extract from rich text widgets
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
            self.INCLUDE_CHILD_WORKSPACES = child_workspaces_value in ("true", "1", "yes", "on")
        
        # Debug logging flag
        if debug_workspace_processing is not None:
            self.DEBUG_WORKSPACE_PROCESSING = debug_workspace_processing
        else:
            debug_value = getenv("DEBUG_WORKSPACE_PROCESSING", "False").lower()
            self.DEBUG_WORKSPACE_PROCESSING = debug_value in ("true", "1", "yes", "on")
        
        # Feature flag: enable/disable rich text extraction
        if enable_rich_text_extraction is not None:
            self._enable_rich_text_extraction = enable_rich_text_extraction
        else:
            rich_text_value = getenv("ENABLE_RICH_TEXT_EXTRACTION", "False").lower()
            self._enable_rich_text_extraction = rich_text_value in ("true", "1", "yes", "on")
        
        # Child workspace data selection
        if child_workspace_data_types is not None:
            self.CHILD_WORKSPACE_DATA_TYPES = child_workspace_data_types
        else:
            child_data_types = getenv("CHILD_WORKSPACE_DATA_TYPES")
            if child_data_types:
                self.CHILD_WORKSPACE_DATA_TYPES = [dt.strip().lower() for dt in child_data_types.split(",") if dt.strip()]
            else:
                self.CHILD_WORKSPACE_DATA_TYPES = []
        
        # Maximum number of parallel workers for child workspace processing
        self.MAX_PARALLEL_WORKSPACES = max_parallel_workspaces or int(getenv("MAX_PARALLEL_WORKSPACES", "5"))
        
        # Dynamic workspace ID
        self._workspace_id = self._WORKSPACE_ID
        self._include_child_workspaces = self.INCLUDE_CHILD_WORKSPACES

    @property
    def WORKSPACE_ID(self):
        """Get current workspace ID."""
        return getattr(self, '_workspace_id', self._WORKSPACE_ID)

    @WORKSPACE_ID.setter
    def WORKSPACE_ID(self, value):
        """Set workspace ID."""
        self._workspace_id = value

    @property
    def INCLUDE_CHILD_WORKSPACES(self):
        return getattr(self, '_include_child_workspaces', False)

    @INCLUDE_CHILD_WORKSPACES.setter
    def INCLUDE_CHILD_WORKSPACES(self, value):
        self._include_child_workspaces = bool(value)

    @property
    def ENABLE_RICH_TEXT_EXTRACTION(self):
        base_value = getattr(self, '_enable_rich_text_extraction', False)
        # When multi-workspace mode is on, always disable rich-text extraction
        if getattr(self, '_include_child_workspaces', False):
            return False
        return base_value

    @ENABLE_RICH_TEXT_EXTRACTION.setter
    def ENABLE_RICH_TEXT_EXTRACTION(self, value):
        self._enable_rich_text_extraction = bool(value)

