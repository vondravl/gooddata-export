"""Common utilities for GoodData API interaction."""


def get_api_client(config):
    """Create API client with configuration.

    Args:
        config: ExportConfig instance with BASE_URL, WORKSPACE_ID, BEARER_TOKEN

    Returns:
        dict: API client configuration
    """
    return {
        "base_url": config.BASE_URL,
        "workspace_id": config.WORKSPACE_ID,
        "headers": {
            "Authorization": f"Bearer {config.BEARER_TOKEN}",
            "X-GDC-VALIDATE-RELATIONS": "true",
        },
        "params": {"origin": "ALL", "size": "2000"},
    }
