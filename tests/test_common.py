"""Tests for gooddata_export/common.py error handling utilities."""

from unittest.mock import MagicMock

import pytest
import requests

from gooddata_export.common import (
    configure_logging,
    get_api_client,
    raise_for_api_error,
    raise_for_connection_error,
    raise_for_request_error,
)


class TestRaiseForApiError:
    """Tests for raise_for_api_error function."""

    def _make_response(
        self, status_code: int, text: str = "Error details"
    ) -> MagicMock:
        """Create a mock response object."""
        response = MagicMock(spec=requests.Response)
        response.status_code = status_code
        response.text = text
        return response

    def test_401_error_message(self):
        """401 errors should mention API token."""
        response = self._make_response(401, "Unauthorized")

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_api_error(response, "metrics")

        error_msg = str(exc_info.value)
        assert "Authentication failed" in error_msg
        assert "metrics" in error_msg
        assert "API token" in error_msg
        assert "Unauthorized" in error_msg

    def test_403_error_message(self):
        """403 errors should mention permissions."""
        response = self._make_response(403, "Forbidden")

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_api_error(response, "dashboards")

        error_msg = str(exc_info.value)
        assert "Access denied" in error_msg
        assert "dashboards" in error_msg
        assert "permissions" in error_msg

    def test_404_error_with_workspace_id(self):
        """404 errors with workspace_id should include workspace info."""
        response = self._make_response(404, "Not found")

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_api_error(response, "metrics", workspace_id="my-workspace-123")

        error_msg = str(exc_info.value)
        assert "404" in error_msg
        assert "metrics" in error_msg
        assert "my-workspace-123" in error_msg
        assert "workspace ID" in error_msg

    def test_404_error_without_workspace_id(self):
        """404 errors without workspace_id should mention API access."""
        response = self._make_response(404, "Not found")

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_api_error(response, "child workspaces")

        error_msg = str(exc_info.value)
        assert "404" in error_msg
        assert "child workspaces" in error_msg
        assert "API access permissions" in error_msg

    def test_500_error_message(self):
        """5xx errors should show HTTP status code."""
        response = self._make_response(500, "Internal Server Error")

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_api_error(response, "visualizations")

        error_msg = str(exc_info.value)
        assert "HTTP 500" in error_msg
        assert "visualizations" in error_msg

    def test_502_error_message(self):
        """502 errors should show HTTP status code."""
        response = self._make_response(502, "Bad Gateway")

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_api_error(response, "ldm")

        error_msg = str(exc_info.value)
        assert "HTTP 502" in error_msg

    def test_response_truncation_at_200_chars(self):
        """Response text should be truncated to 200 characters."""
        long_response = "x" * 500
        response = self._make_response(500, long_response)

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_api_error(response, "metrics")

        error_msg = str(exc_info.value)
        # Should contain exactly 200 x's, not 500
        assert "x" * 200 in error_msg
        assert "x" * 201 not in error_msg

    def test_response_under_200_chars_not_truncated(self):
        """Response text under 200 chars should not be truncated."""
        short_response = "Short error message"
        response = self._make_response(400, short_response)

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_api_error(response, "metrics")

        error_msg = str(exc_info.value)
        assert "Short error message" in error_msg


class TestRaiseForConnectionError:
    """Tests for raise_for_connection_error function."""

    def test_basic_connection_error(self):
        """Basic connection error with minimal params."""
        error = ConnectionError("Connection refused")

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_connection_error("metrics", error)

        error_msg = str(exc_info.value)
        assert "Connection error" in error_msg
        assert "metrics" in error_msg
        assert "Connection refused" in error_msg

    def test_connection_error_with_base_url(self):
        """Connection error should include base_url hint."""
        error = ConnectionError("Connection refused")

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_connection_error(
                "dashboards", error, base_url="https://example.gooddata.com"
            )

        error_msg = str(exc_info.value)
        assert "https://example.gooddata.com" in error_msg
        assert "BASE_URL" in error_msg

    def test_connection_error_with_retry_info(self):
        """Connection error should include retry information."""
        error = TimeoutError("Request timed out")

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_connection_error("metrics", error, retry_info="after 4 attempts")

        error_msg = str(exc_info.value)
        assert "after 4 attempts" in error_msg

    def test_connection_error_unknown_base_url(self):
        """Connection error without base_url should show 'unknown'."""
        error = ConnectionError("Connection refused")

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_connection_error("metrics", error, base_url=None)

        error_msg = str(exc_info.value)
        assert "unknown" in error_msg


class TestRaiseForRequestError:
    """Tests for raise_for_request_error function."""

    def test_timeout_error_dispatches_to_connection_handler(self):
        """Timeout errors should produce connection error message."""
        error = requests.exceptions.Timeout("Request timed out")

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_request_error("metrics", error, base_url="https://test.com")

        error_msg = str(exc_info.value)
        assert "Connection error" in error_msg
        assert "BASE_URL" in error_msg

    def test_connection_error_dispatches_to_connection_handler(self):
        """ConnectionError should produce connection error message."""
        error = requests.exceptions.ConnectionError("Connection refused")

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_request_error("dashboards", error)

        error_msg = str(exc_info.value)
        assert "Connection error" in error_msg

    def test_other_request_exception_generic_message(self):
        """Other RequestExceptions should produce generic message."""
        error = requests.exceptions.RequestException("Something went wrong")

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_request_error("visualizations", error)

        error_msg = str(exc_info.value)
        assert "Request failed" in error_msg
        assert "visualizations" in error_msg
        assert "Something went wrong" in error_msg

    def test_retry_info_passed_to_connection_handler(self):
        """Retry info should be included in connection errors."""
        error = requests.exceptions.Timeout("Timeout")

        with pytest.raises(RuntimeError) as exc_info:
            raise_for_request_error("metrics", error, retry_info="after 3 retries")

        error_msg = str(exc_info.value)
        assert "after 3 retries" in error_msg


class TestGetApiClient:
    """Tests for get_api_client function."""

    def test_returns_existing_client(self):
        """When client is provided, return it as-is."""
        existing_client = {
            "base_url": "https://existing.com",
            "workspace_id": "existing-ws",
            "headers": {"Authorization": "Bearer existing"},
        }

        result = get_api_client(client=existing_client)

        assert result is existing_client

    def test_creates_client_from_config(self):
        """When only config is provided, create new client."""
        mock_config = MagicMock()
        mock_config.BASE_URL = "https://new.gooddata.com"
        mock_config.WORKSPACE_ID = "new-workspace"
        mock_config.BEARER_TOKEN = "new-token"

        result = get_api_client(config=mock_config)

        assert result["base_url"] == "https://new.gooddata.com"
        assert result["workspace_id"] == "new-workspace"
        assert result["headers"]["Authorization"] == "Bearer new-token"
        assert "X-GDC-VALIDATE-RELATIONS" in result["headers"]
        assert "params" in result

    def test_client_takes_precedence_over_config(self):
        """When both client and config provided, client wins."""
        existing_client = {
            "base_url": "https://client.com",
            "workspace_id": "client-ws",
        }
        mock_config = MagicMock()
        mock_config.BASE_URL = "https://config.com"

        result = get_api_client(config=mock_config, client=existing_client)

        assert result is existing_client
        assert result["base_url"] == "https://client.com"

    def test_raises_without_client_or_config(self):
        """Should raise ValueError when neither client nor config provided."""
        with pytest.raises(ValueError) as exc_info:
            get_api_client()

        assert "Either client or config must be provided" in str(exc_info.value)

    def test_raises_with_none_values(self):
        """Should raise ValueError when both are explicitly None."""
        with pytest.raises(ValueError):
            get_api_client(config=None, client=None)

    def test_client_params_include_origin_and_size(self):
        """Created client should have standard params."""
        mock_config = MagicMock()
        mock_config.BASE_URL = "https://test.com"
        mock_config.WORKSPACE_ID = "ws"
        mock_config.BEARER_TOKEN = "token"

        result = get_api_client(config=mock_config)

        assert result["params"]["origin"] == "ALL"
        assert result["params"]["size"] == "2000"


class TestConfigureLogging:
    """Tests for configure_logging function."""

    @pytest.fixture(autouse=True)
    def reset_logging(self):
        """Restore logging state after each test to avoid affecting other tests."""
        import logging

        root = logging.getLogger()
        original_level = root.level
        original_handlers = root.handlers[:]
        yield
        root.setLevel(original_level)
        root.handlers = original_handlers

    def test_debug_true_sets_debug_level(self):
        """When debug=True, root logger should be set to DEBUG level."""
        import logging

        configure_logging(debug=True)

        assert logging.getLogger().level == logging.DEBUG

    def test_debug_false_sets_info_level(self):
        """When debug=False, root logger should be set to INFO level."""
        import logging

        configure_logging(debug=False)

        assert logging.getLogger().level == logging.INFO

    def test_format_is_message_only(self):
        """Logging format should be message-only (no timestamps/levels)."""
        import logging

        configure_logging(debug=False)

        # Check that the root logger's handler uses the expected format
        root_logger = logging.getLogger()
        # basicConfig adds a StreamHandler to root logger
        for handler in root_logger.handlers:
            if handler.formatter:
                assert handler.formatter._fmt == "%(message)s"
                break
        else:
            pytest.fail("No handler with formatter found on root logger")
