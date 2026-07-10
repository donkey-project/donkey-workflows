try:
    import donkey.core  # noqa: F401

except ImportError as e:
    raise ImportError(
        "Missing dependency 'donkey-core' required for prebuilt workflows. "
        "Install with `pip install donkey-workflows[prebuilt]`."
    ) from e


from donkey_workflows.prebuilt.document_ingestion import DocumentIngestionWorkflow

__all__ = ["DocumentIngestionWorkflow"]
