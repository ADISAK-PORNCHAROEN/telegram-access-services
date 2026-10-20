"""Requester-only reads for durable copy jobs."""

from app.application.ports import AccessStore, JobStore
from app.domain.models import CopyJobSnapshot


class GetCopyJobStatus:
    def __init__(self, store: JobStore, access: AccessStore):
        self.store, self.access = store, access

    async def execute(self, requester_id: int, job_id: str) -> CopyJobSnapshot | None:
        if not self.access.is_allowed(requester_id):
            return None
        job = await self.store.get_job(job_id)
        if job is None or job.requester_id != requester_id:
            return None
        return job
