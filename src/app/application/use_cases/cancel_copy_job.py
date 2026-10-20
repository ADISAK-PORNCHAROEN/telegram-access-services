"""Requester-only durable job cancellation."""

from app.application.ports import AccessStore, JobRunner, JobStore
from app.domain.models import (
    JobCancellationDecision, JobCancellationDisposition, JobState,
)


class CancelCopyJob:
    def __init__(self, store: JobStore, access: AccessStore, runner: JobRunner):
        self.store, self.access, self.runner = store, access, runner

    async def execute(self, requester_id: int,
                      job_id: str) -> JobCancellationDecision:
        if not self.access.is_allowed(requester_id):
            return JobCancellationDecision(JobCancellationDisposition.NOT_FOUND)
        job = await self.store.get_job(job_id)
        if job is None or job.requester_id != requester_id:
            return JobCancellationDecision(JobCancellationDisposition.NOT_FOUND)

        decision = await self.store.request_cancel(job_id)
        if decision.disposition == JobCancellationDisposition.NOT_FOUND:
            return decision
        if decision.stop_worker:
            await self.runner.cancel(job_id)
        if decision.disposition == JobCancellationDisposition.CANCELLATION_REQUESTED:
            updated = await self.store.get_job(job_id)
            if updated is not None and updated.state == JobState.CANCELLED:
                return JobCancellationDecision(
                    JobCancellationDisposition.CANCELLED, JobState.CANCELLED)
        return decision
