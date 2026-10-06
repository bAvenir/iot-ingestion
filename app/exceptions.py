"""Application error types.

Kept separate from the storage/queue layers so callers can branch on *meaning*
(retryable vs not) instead of re-inspecting a boto3 error response.
"""


class IngestionError(Exception):
    """Base for everything this application raises deliberately."""


class ObjectNotFound(IngestionError):
    """A referenced object is absent from object storage.

    Permanent: retrying cannot help, so the job is dead-lettered rather than
    requeued.
    """


class MapperConfigError(IngestionError):
    """Base class for all Mapper configuration errors."""


class RowRejected(IngestionError):
    """A row cannot become a silver row. Carries a dead-letter reason code."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail
