"""Common utilities for GoodData API interaction."""

import logging
from typing import Any, NoReturn

import requests

logger = logging.getLogger(__name__)


def configure_logging(debug: bool) -> None:
    """Configure logging level based on debug flag.

    Args:
        debug: If True, set DEBUG level; otherwise INFO level.
    """
    level = logging.DEBUG if debug else logging.INFO
    # force=True allows reconfiguring after import-time basicConfig calls
    logging.basicConfig(level=level, format="%(message)s", force=True)


class ExportError(Exception):
    """Exception raised for errors during the export process.

    This exception is used when export operations fail due to
    data processing errors, API errors, or configuration issues.
    """

    pass


def raise_for_connection_error(
    context: str,
    error: Exception,
    base_url: str | None = None,
    retry_info: str | None = None,
) -> NoReturn:
    """Raise RuntimeError with detailed message for connection errors.

    Args:
        context: Description of what was being fetched (e.g., "metrics", "child workspaces")
        error: The caught exception
        base_url: Optional base URL to include in error hint
        retry_info: Optional retry information (e.g., "after 4 attempts")

    Raises:
        RuntimeError: Always raises with appropriate error message
    """
    url_hint = base_url if base_url else "unknown"
    retry_suffix = f" {retry_info}" if retry_info else ""

    error_msg = (
        f"Connection error fetching {context}{retry_suffix}\n"
        f"Please verify BASE_URL in .env file: {url_hint}\n"
        f"Error: {error}"
    )
    logger.error(error_msg)
    raise RuntimeError(error_msg)


def raise_for_api_error(
    response: requests.Response,
    context: str,
    workspace_id: str | None = None,
) -> NoReturn:
    """Raise RuntimeError with detailed message for HTTP errors.

    This is a terminal function that always raises - it never returns.
    Use it as the final handler for non-2xx responses.

    Args:
        response: The HTTP response object
        context: Description of what was being fetched (e.g., "metrics", "child workspaces")
        workspace_id: Optional workspace ID to include in 404 errors

    Raises:
        RuntimeError: Always raises with appropriate error message
    """
    status = response.status_code
    response_text = response.text

    # Truncate response for all error messages (consistent 200 char limit)
    truncated_response = response_text[:200]

    if status == 404:
        if workspace_id:
            error_msg = (
                f"Failed to fetch {context} (404)\n"
                f"Workspace: {workspace_id}\n"
                f"Please verify workspace ID in .env file\n"
                f"Response: {truncated_response}"
            )
        else:
            error_msg = (
                f"Failed to fetch {context} (404)\n"
                f"Please verify API access permissions\n"
                f"Response: {truncated_response}"
            )
    elif status == 401:
        error_msg = (
            f"Authentication failed for {context}\n"
            f"Please check API token in .env file\n"
            f"Response: {truncated_response}"
        )
    elif status == 403:
        error_msg = (
            f"Access denied to {context}\n"
            f"Please check API permissions\n"
            f"Response: {truncated_response}"
        )
    else:
        error_msg = (
            f"Failed to fetch {context} (HTTP {status})\nResponse: {truncated_response}"
        )

    logger.error(error_msg)
    raise RuntimeError(error_msg)


def raise_for_request_error(
    context: str,
    error: Exception,
    base_url: str | None = None,
    retry_info: str | None = None,
) -> NoReturn:
    """Unified handler for requests exceptions.

    Dispatches to appropriate handler based on exception type:
    - Timeout/ConnectionError: User-friendly "check HOST" message
    - Other RequestException: Generic "request failed" message

    Args:
        context: Description of what was being fetched (e.g., "metrics")
        error: The caught exception
        base_url: Optional base URL to include in connection error hints
        retry_info: Optional retry information (e.g., "after 4 attempts")

    Raises:
        RuntimeError: Always raises with appropriate error message
    """
    if isinstance(
        error, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)
    ):
        raise_for_connection_error(context, error, base_url, retry_info)

    error_msg = f"Request failed for {context}: {error}"
    logger.error(error_msg)
    raise RuntimeError(error_msg)


def get_api_client(*, client=None, config=None) -> dict[str, Any]:
    """Get existing client or create new one from config.

    Args:
        client: Existing API client dict (returned as-is if provided)
        config: ExportConfig instance (used to create new client)

    Returns:
        dict: API client configuration

    Raises:
        ValueError: If neither client nor config is provided
    """
    if client is not None:
        return client
    if config is None:
        raise ValueError("Either client or config must be provided")
    logger.debug("Created new API client")
    return {
        "base_url": config.BASE_URL,
        "workspace_id": config.WORKSPACE_ID,
        "headers": {
            "Authorization": f"Bearer {config.BEARER_TOKEN}",
            "X-GDC-VALIDATE-RELATIONS": "true",
        },
        "params": {"origin": "ALL", "size": "2000"},
    }
