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
