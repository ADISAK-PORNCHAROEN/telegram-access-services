"""asyncpg adapter for committed copy-job admission and execution state."""

import json
from uuid import uuid4

from app.domain.models import (
    CopyJobInput, CopyJobSnapshot, DeliveryIntent, DeliveryOutcome, JobAdmission,
    JobCancellationDecision, JobCancellationDisposition, JobState, ParsedLink,
)

ADVISORY_LOCK_KEY = 731_642_019
JOB_LEASE_SECONDS = 30
VALID_STAGES = {"resolve", "download", "upload", "send"}
VALID_TERMINAL_STATES = {
    "completed", "partial", "failed", "expired", "delivery_unknown",
}


def _json_value(value):
    return json.loads(value) if isinstance(value, str) else value


def _intent_payload(intent: DeliveryIntent | None) -> dict:
    if intent is None:
        return {"kind": "media", "items": []}
    return {
        "kind": intent.kind,
        "items": [
            {"position": item.position, "item_id": item.item_id,
             "size_bytes": item.size_bytes}
            for item in intent.items
        ],
    }


def _outcome_payload(outcome: DeliveryOutcome) -> dict:
    return outcome.as_dict()


def _merge_delivery_summary(result: dict, summary: dict) -> dict:
    merged = dict(result)
    merged["delivery_outcomes"] = [
        _outcome_payload(outcome) for outcome in summary["delivery_outcomes"]]
    if summary["total_files"] is not None:
        merged["sent_files"] = summary["sent_files"]
        merged["total_files"] = summary["total_files"]
        stats = dict(merged.get("stats") or {})
        stats["delivered_bytes"] = summary["delivered_bytes"]
        merged["stats"] = stats
    if summary["unresolved"]:
        merged["status"] = "delivery_unknown"
        merged["reason"] = merged.get("reason") or "unresolved-send-intent"
    return merged


def _interrupted_delivery_state(summary: dict) -> tuple[JobState, str, str]:
    if summary["unresolved"]:
        return (JobState.DELIVERY_UNKNOWN, "delivery_unknown",
                "process-restarted-after-send-intent")
    if summary["total_files"] is not None:
        sent = summary["sent_files"] or 0
        total = summary["total_files"]
        if total and sent >= total:
            return JobState.COMPLETED, "ok", "confirmed-delivery-before-finalize"
        if sent:
            return JobState.PARTIAL, "partial", "confirmed-partial-before-finalize"
        return JobState.FAILED, "fail", "definitive-send-rejection-before-finalize"
    if summary["text_sent"]:
        return JobState.COMPLETED, "text", "confirmed-text-before-finalize"
    return JobState.FAILED, "fail", "definitive-send-rejection-before-finalize"


class JobStateConflict(RuntimeError):
    """A claimed job changed state before the requested durable transition."""


class PostgresJobStore:
    def __init__(self, pool):
        self.pool = pool

    async def accept(self, job: CopyJobInput, *, max_pending: int,
                     max_pending_per_user: int) -> JobAdmission:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # Serialize admission checks so parallel Telegram update handlers cannot
                # exceed either configured pending-job limit.
                await connection.execute("SELECT pg_advisory_xact_lock($1)",
                                         ADVISORY_LOCK_KEY)
                existing = await connection.fetchrow(
                    """SELECT job_id FROM copy_jobs
                       WHERE receiving_bot_id = $1 AND destination_chat = $2
                         AND request_message_id = $3 AND link_index = $4""",
                    job.receiving_bot_id, job.destination_chat,
                    job.request_message_id, job.link_index)
                if existing:
                    return JobAdmission(True, existing["job_id"], replayed=True)

                pending = await connection.fetchval(
                    "SELECT count(*) FROM copy_jobs WHERE state = 'queued'")
                if pending >= max_pending:
                    return JobAdmission(False, rejection_reason="global-pending-limit")
                user_pending = await connection.fetchval(
                    "SELECT count(*) FROM copy_jobs WHERE state = 'queued' AND requester_id = $1",
                    job.requester_id)
                if user_pending >= max_pending_per_user:
                    return JobAdmission(False, rejection_reason="user-pending-limit")

                accepted_at = await connection.fetchval("SELECT clock_timestamp()")
                await connection.execute(
                    """INSERT INTO copy_jobs (
                           job_id, receiving_bot_id, requester_id, destination_chat,
                           request_message_id, link_index, source_chat,
                           source_message_id, source_is_private, filter_kind,
                           state, stage, created_at, deadline_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                               'queued', 'queued', $11::timestamptz,
                               $11::timestamptz + interval '30 minutes')""",
                    job.job_id, job.receiving_bot_id, job.requester_id,
                    job.destination_chat, job.request_message_id, job.link_index,
                    str(job.source.chat), job.source.message_id,
                    job.source.is_private, job.filter_kind, accepted_at)
                return JobAdmission(True, job.job_id)

    async def get_job(self, job_id: str) -> CopyJobSnapshot | None:
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(
                """SELECT job_id, requester_id, state, stage, created_at,
                          updated_at, next_eligible_at, rate_limit_kind,
                          rate_limit_account, rate_limit_required_seconds, result
                   FROM copy_jobs WHERE job_id = $1""", job_id)
        if row is None:
            return None
        result = row["result"]
        if isinstance(result, str):
            result = json.loads(result)
        return CopyJobSnapshot(
            job_id=row["job_id"], requester_id=row["requester_id"],
            state=JobState(row["state"]), stage=row["stage"],
            created_at=row["created_at"], updated_at=row["updated_at"],
            next_eligible_at=row["next_eligible_at"], result=result,
            rate_limit_kind=row["rate_limit_kind"],
            rate_limit_account=row["rate_limit_account"],
            rate_limit_required_seconds=row["rate_limit_required_seconds"],
        )

    async def request_cancel(self, job_id: str) -> JobCancellationDecision:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """SELECT state, send_intent_at FROM copy_jobs
                       WHERE job_id = $1 FOR UPDATE""", job_id)
                if row is None:
                    return JobCancellationDecision(
                        JobCancellationDisposition.NOT_FOUND)

                state = JobState(row["state"])
                if state == JobState.CANCELLED:
                    return JobCancellationDecision(
                        JobCancellationDisposition.ALREADY_CANCELLED, state)
                if state in {
                        JobState.COMPLETED, JobState.PARTIAL, JobState.FAILED,
                        JobState.EXPIRED, JobState.DELIVERY_UNKNOWN}:
                    return JobCancellationDecision(
                        JobCancellationDisposition.ALREADY_FINISHED, state)
                if row["send_intent_at"] is not None:
                    return JobCancellationDecision(
                        JobCancellationDisposition.TOO_LATE, state)

                if state in {JobState.QUEUED, JobState.WAITING_RATE_LIMIT}:
                    await connection.execute(
                        """UPDATE copy_jobs SET state = 'cancelled', stage = 'terminal',
                                  cancel_requested_at = COALESCE(cancel_requested_at, now()),
                                  next_eligible_at = NULL,
                                  result = jsonb_build_object(
                                      'status', 'cancelled',
                                      'reason', 'requester-cancelled',
                                      'sent_files', 0, 'total_files', 0),
                                  updated_at = now()
                           WHERE job_id = $1""", job_id)
                    return JobCancellationDecision(
                        JobCancellationDisposition.CANCELLED, JobState.CANCELLED,
                        stop_worker=state == JobState.WAITING_RATE_LIMIT)

                if state == JobState.RUNNING:
                    await connection.execute(
                        """UPDATE copy_jobs
                           SET cancel_requested_at = COALESCE(cancel_requested_at, now()),
                               updated_at = now()
                           WHERE job_id = $1 AND state = 'running'""", job_id)
                    return JobCancellationDecision(
                        JobCancellationDisposition.CANCELLATION_REQUESTED,
                        state, stop_worker=True)

                return JobCancellationDecision(
                    JobCancellationDisposition.ALREADY_FINISHED, state)

    async def claim_next(self) -> CopyJobInput | None:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # Serialize requester fairness and the one-active-job rule across
                # worker tasks and process instances sharing this database.
                await connection.execute("SELECT pg_advisory_xact_lock($1)",
                                         ADVISORY_LOCK_KEY)
                await self._recover_expired_leases(connection)
                await connection.execute(
                    """UPDATE copy_jobs SET state = 'expired', stage = 'terminal',
                              next_eligible_at = NULL, lease_expires_at = NULL,
                              result = jsonb_build_object(
                                  'status', 'expired',
                                  'reason', 'accepted-job-deadline'),
                              updated_at = clock_timestamp()
                       WHERE state = 'queued'
                         AND deadline_at <= clock_timestamp()
                         AND send_intent_at IS NULL""")
                row = await connection.fetchrow(
                    """SELECT candidate.job_id, candidate.receiving_bot_id,
                              candidate.requester_id, candidate.destination_chat,
                              candidate.deadline_at, candidate.updated_at AS enqueued_at,
                              EXTRACT(EPOCH FROM (
                                  candidate.deadline_at - clock_timestamp()))
                                  ::double precision AS deadline_remaining_seconds,
                              candidate.request_message_id, candidate.link_index,
                              candidate.source_chat, candidate.source_message_id,
                              candidate.source_is_private, candidate.filter_kind
                       FROM copy_jobs AS candidate
                       WHERE candidate.state = 'queued'
                         AND (candidate.next_eligible_at IS NULL
                              OR candidate.next_eligible_at <= clock_timestamp())
                         AND candidate.deadline_at > clock_timestamp()
                         AND NOT EXISTS (
                             SELECT 1 FROM copy_jobs AS active
                             WHERE active.requester_id = candidate.requester_id
                               AND active.state IN ('running', 'waiting_rate_limit'))
                    ORDER BY (SELECT max(history.claimed_at)
                                 FROM copy_jobs AS history
                                 WHERE history.requester_id = candidate.requester_id)
                                    ASC NULLS FIRST,
                                (SELECT min(waiting.created_at)
                                 FROM copy_jobs AS waiting
                                 WHERE waiting.requester_id = candidate.requester_id
                                   AND waiting.state = 'queued'),
                                candidate.created_at, candidate.job_id
                       LIMIT 1 FOR UPDATE OF candidate SKIP LOCKED""")
                if row is None:
                    return None
                claimed = await connection.fetchrow(
                    """UPDATE copy_jobs SET state = 'running', stage = 'resolve',
                              next_eligible_at = NULL,
                              lease_generation = lease_generation + 1,
                              lease_expires_at = clock_timestamp()
                                  + make_interval(secs => $2),
                              claimed_at = clock_timestamp(),
                              updated_at = clock_timestamp()
                       WHERE job_id = $1
                       RETURNING lease_generation,
                                 EXTRACT(EPOCH FROM
                                     (clock_timestamp() - $3::timestamptz))
                                     ::double precision AS claim_queue_wait_seconds""",
                    row["job_id"], JOB_LEASE_SECONDS, row["enqueued_at"])
                if claimed is None:
                    return None
                source_chat = row["source_chat"]
                if source_chat.lstrip("-").isdigit():
                    source_chat = int(source_chat)
                return CopyJobInput(
                    job_id=row["job_id"],
                    receiving_bot_id=row["receiving_bot_id"],
                    requester_id=row["requester_id"],
                    destination_chat=row["destination_chat"],
                    request_message_id=row["request_message_id"],
                    link_index=row["link_index"],
                    source=ParsedLink(source_chat, row["source_message_id"],
                                      row["source_is_private"]),
                    filter_kind=row["filter_kind"],
                    deadline_at=row["deadline_at"],
                    deadline_remaining_seconds=row[
                        "deadline_remaining_seconds"],
                    lease_generation=claimed["lease_generation"],
                    claim_queue_wait_seconds=max(
                        0.0, float(claimed["claim_queue_wait_seconds"])),
                )

    async def _recover_expired_leases(self, connection) -> None:
        """Recover only work whose database-clock lease has expired."""
        rows = await connection.fetch(
            """SELECT job_id, lease_generation, send_intent_at,
                      cancel_requested_at, deadline_at
               FROM copy_jobs
               WHERE state IN ('running', 'waiting_rate_limit')
                 AND lease_expires_at <= clock_timestamp()
               FOR UPDATE SKIP LOCKED""")
        for row in rows:
            summary = await self._delivery_summary(connection, row["job_id"])
            if row["send_intent_at"] is not None:
                state, status, reason = _interrupted_delivery_state(summary)
                result = {"status": status, "reason": reason}
            elif row["cancel_requested_at"] is not None:
                state = JobState.CANCELLED
                result = {"status": state.value,
                          "reason": "requester-cancelled",
                          "sent_files": 0, "total_files": 0}
            elif row["deadline_at"] <= await connection.fetchval(
                    "SELECT clock_timestamp()"):
                state = JobState.EXPIRED
                result = {"status": state.value,
                          "reason": "accepted-job-deadline"}
            else:
                state, result = JobState.QUEUED, None

            if result is not None:
                result = _merge_delivery_summary(result, summary)
            await connection.execute(
                """UPDATE copy_jobs SET state = $3, stage = $4,
                          next_eligible_at = CASE WHEN $3 = 'queued'
                              THEN next_eligible_at ELSE NULL END,
                          lease_expires_at = NULL, result = $5::jsonb,
                          updated_at = clock_timestamp()
                   WHERE job_id = $1 AND lease_generation = $2
                     AND lease_expires_at <= clock_timestamp()""",
                row["job_id"], row["lease_generation"], state.value,
                "queued" if state == JobState.QUEUED else "terminal",
                json.dumps(result, separators=(",", ":"), ensure_ascii=False)
                if result is not None else None)

    async def heartbeat(self, job_id: str, lease_generation: int) -> bool:
        async with self.pool.acquire() as connection:
            status = await connection.execute(
                """UPDATE copy_jobs
                   SET lease_expires_at = clock_timestamp() + make_interval(secs => $3),
                       updated_at = clock_timestamp()
                   WHERE job_id = $1 AND lease_generation = $2
                     AND state IN ('running', 'waiting_rate_limit')
                     AND lease_expires_at > clock_timestamp()""",
                job_id, lease_generation, JOB_LEASE_SECONDS)
        return status == "UPDATE 1"

    async def set_stage(self, job_id: str, stage: str,
                        lease_generation: int) -> None:
        if stage not in VALID_STAGES:
            raise ValueError(f"invalid durable job stage: {stage}")
        async with self.pool.acquire() as connection:
            status = await connection.execute(
                """UPDATE copy_jobs SET stage = $2, updated_at = now()
                   WHERE job_id = $1 AND state = 'running'
                     AND cancel_requested_at IS NULL
                     AND lease_generation = $3
                     AND lease_expires_at > clock_timestamp()""",
                job_id, stage, lease_generation)
        if status != "UPDATE 1":
            raise JobStateConflict("job lease is no longer owned")

    async def mark_send_intent(self, job_id: str,
                               lease_generation: int,
                               intent: DeliveryIntent | None = None) -> str | None:
        attempt_id = uuid4().hex
        encoded = json.dumps(_intent_payload(intent), separators=(",", ":"))
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                marked = await connection.fetchval(
                    """UPDATE copy_jobs SET stage = 'send',
                              send_intent_at = COALESCE(send_intent_at, now()),
                              updated_at = now()
                       WHERE job_id = $1 AND state = 'running'
                         AND cancel_requested_at IS NULL
                         AND lease_generation = $2
                         AND lease_expires_at > clock_timestamp()
                       RETURNING job_id""", job_id, lease_generation)
                if marked is None:
                    return None
                await connection.execute(
                    """INSERT INTO copy_delivery_attempts
                           (attempt_id, job_id, intent, state)
                       VALUES ($1, $2, $3::jsonb, 'intent')""",
                    attempt_id, job_id, encoded)
        return attempt_id

    async def record_delivery_outcomes(
            self, job_id: str, attempt_id: str,
            outcomes: tuple[DeliveryOutcome, ...],
            lease_generation: int) -> None:
        incoming = [_outcome_payload(outcome) for outcome in outcomes]
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                owner = await connection.fetchval(
                    """SELECT job_id FROM copy_jobs
                       WHERE job_id = $1 AND lease_generation = $2
                         AND state IN ('running', 'waiting_rate_limit')
                         AND lease_expires_at > clock_timestamp()
                       FOR UPDATE""", job_id, lease_generation)
                if owner is None:
                    raise JobStateConflict("job lease is no longer owned")
                current = await connection.fetchrow(
                    """SELECT intent, state, outcomes FROM copy_delivery_attempts
                       WHERE job_id = $1 AND attempt_id = $2 FOR UPDATE""",
                    job_id, attempt_id)
                if current is None:
                    raise JobStateConflict("delivery attempt is missing")
                existing = _json_value(current["outcomes"])
                combined = {}
                for outcome in (*existing, *incoming):
                    key = (outcome.get("position"), outcome.get("item_id"))
                    previous = combined.get(key)
                    if previous is not None and previous != outcome:
                        raise JobStateConflict(
                            "delivery attempt has conflicting item outcomes")
                    combined[key] = outcome
                combined_outcomes = list(combined.values())
                encoded = json.dumps(combined_outcomes, separators=(",", ":"),
                                     ensure_ascii=False)
                if current["state"] != "intent":
                    if (_json_value(current["outcomes"]) == combined_outcomes
                            and current["state"] in {"confirmed", "rejected"}):
                        return
                    raise JobStateConflict(
                        "delivery attempt is no longer unresolved")

                intent = _json_value(current["intent"])
                items = intent.get("items", [])
                expected = {(item["position"], item["item_id"])
                            for item in items}
                if intent.get("kind") == "text":
                    expected = {(None, None)}
                complete = bool(combined) and (
                    expected.issubset(combined) if expected else True)
                rejected = complete and all(
                    outcome.get("status") == "rejected"
                    for outcome in combined_outcomes)
                state = "rejected" if rejected else "confirmed" if complete else "intent"
                await connection.execute(
                    """UPDATE copy_delivery_attempts SET state = $3,
                              outcomes = $4::jsonb,
                              resolved_at = CASE WHEN $3 = 'intent' THEN NULL ELSE now() END
                       WHERE job_id = $1 AND attempt_id = $2""",
                    job_id, attempt_id, state, encoded)

    async def delivery_summary(self, job_id: str) -> dict:
        async with self.pool.acquire() as connection:
            return await self._delivery_summary(connection, job_id)

    async def _delivery_summary(self, connection, job_id: str) -> dict:
        rows = await connection.fetch(
            """SELECT intent, state, outcomes FROM copy_delivery_attempts
               WHERE job_id = $1 ORDER BY created_at, attempt_id""", job_id)
        send_intent_at = await connection.fetchval(
            "SELECT send_intent_at FROM copy_jobs WHERE job_id = $1", job_id)
        confirmed = {}
        total_files = None
        text_attempted = False
        text_sent = False
        unresolved = send_intent_at is not None and not rows
        for row in rows:
            intent = _json_value(row["intent"])
            items = intent.get("items", [])
            text_attempted = text_attempted or intent.get("kind") == "text"
            if intent.get("kind") in {"album", "media"} and items:
                total_files = max(total_files or 0, len(items))
            if row["state"] == "intent":
                unresolved = True
            for raw in _json_value(row["outcomes"]):
                if raw.get("status") != "sent":
                    continue
                outcome = DeliveryOutcome(**raw)
                if outcome.position is None:
                    text_sent = True
                key = (outcome.position, outcome.item_id)
                confirmed.setdefault(key, outcome)
        outcomes = tuple(sorted(
            confirmed.values(),
            key=lambda item: (item.position is None, item.position or 0,
                              item.item_id or 0)))
        sent_files = (sum(1 for item in outcomes if item.position is not None)
                      if total_files is not None else None)
        return {
            "delivery_outcomes": outcomes,
            "sent_files": sent_files,
            "total_files": total_files,
            "delivered_bytes": sum(item.delivered_bytes or 0 for item in outcomes),
            "unresolved": unresolved,
            "text_attempted": text_attempted,
            "text_sent": text_sent,
        }

    async def cancellation_requested(self, job_id: str) -> bool:
        async with self.pool.acquire() as connection:
            return bool(await connection.fetchval(
                """SELECT cancel_requested_at IS NOT NULL
                   FROM copy_jobs WHERE job_id = $1""", job_id))

    async def mark_waiting(self, job_id: str, wait_seconds: float,
                           wait_kind: str, wait_account: str,
                           lease_generation: int) -> bool:
        if wait_seconds <= 0:
            raise ValueError("rate limit wait duration must be positive")
        if wait_kind not in {"flood", "premium"}:
            raise ValueError("unsupported rate limit wait kind")
        if wait_account not in {"owner", "bot", "unknown"}:
            raise ValueError("unsupported Telegram account role")
        async with self.pool.acquire() as connection:
            status = await connection.execute(
                """WITH wait_clock AS (
                       SELECT clock_timestamp() AS now
                   )
                   UPDATE copy_jobs AS job
                   SET state = 'waiting_rate_limit',
                       next_eligible_at = GREATEST(
                           COALESCE(job.next_eligible_at, wait_clock.now),
                           wait_clock.now + make_interval(secs => $2::double precision)),
                       rate_limit_kind = $3,
                       rate_limit_account = $4,
                       rate_limit_required_seconds = $2,
                       updated_at = wait_clock.now
                   FROM wait_clock
                   WHERE job.job_id = $1
                     AND job.state IN ('running', 'waiting_rate_limit')
                     AND job.cancel_requested_at IS NULL
                     AND job.send_intent_at IS NULL
                     AND job.lease_generation = $5
                     AND job.lease_expires_at > wait_clock.now""",
                job_id, wait_seconds, wait_kind, wait_account, lease_generation)
        return status == "UPDATE 1"

    async def resume_waiting(self, job_id: str, lease_generation: int) -> bool:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """SELECT state, cancel_requested_at FROM copy_jobs
                       WHERE job_id = $1 AND lease_generation = $2
                         AND lease_expires_at > clock_timestamp()
                       FOR UPDATE""", job_id, lease_generation)
                if row is None or row["cancel_requested_at"] is not None:
                    return False
                if row["state"] == "running":
                    return True
                if row["state"] != "waiting_rate_limit":
                    return False
                status = await connection.execute(
                    """UPDATE copy_jobs SET state = 'running',
                              next_eligible_at = NULL, updated_at = now()
                       WHERE job_id = $1 AND state = 'waiting_rate_limit'
                         AND lease_generation = $2
                         AND lease_expires_at > clock_timestamp()""",
                    job_id, lease_generation)
                return status == "UPDATE 1"

    async def finish(self, job_id: str, state: str, result: dict,
                     lease_generation: int) -> None:
        if state not in VALID_TERMINAL_STATES:
            raise ValueError(f"invalid durable job terminal state: {state}")
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                summary = await self._delivery_summary(connection, job_id)
                durable_result = _merge_delivery_summary(result, summary)
                if summary["unresolved"]:
                    state = JobState.DELIVERY_UNKNOWN.value
                encoded = json.dumps(durable_result, separators=(",", ":"),
                                     ensure_ascii=False)
                status = await connection.execute(
                    """UPDATE copy_jobs SET state = $2, stage = 'terminal',
                              next_eligible_at = NULL, lease_expires_at = NULL,
                              result = $3::jsonb, updated_at = clock_timestamp()
                       WHERE job_id = $1 AND state = 'running'
                         AND cancel_requested_at IS NULL
                         AND lease_generation = $4
                         AND lease_expires_at > clock_timestamp()""",
                    job_id, state, encoded, lease_generation)
        if status != "UPDATE 1":
            raise JobStateConflict("job lease is no longer owned")

    async def expire_deadline(self, job_id: str, result: dict,
                              lease_generation: int) -> JobState | None:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """SELECT state, send_intent_at, cancel_requested_at
                       FROM copy_jobs WHERE job_id = $1
                         AND lease_generation = $2
                         AND lease_expires_at > clock_timestamp()
                       FOR UPDATE""", job_id, lease_generation)
                if row is None or row["state"] not in {
                        "running", "waiting_rate_limit"}:
                    return None

                summary = await self._delivery_summary(connection, job_id)
                if row["send_intent_at"] is not None:
                    state, status, reason = _interrupted_delivery_state(summary)
                    if state == JobState.DELIVERY_UNKNOWN:
                        reason = "deadline-after-send-intent"
                    durable_result = dict(result)
                    durable_result.update(status=status, reason=reason)
                elif row["cancel_requested_at"] is not None:
                    state = JobState.CANCELLED
                    durable_result = dict(result)
                    durable_result.update(status=state.value,
                                          reason="requester-cancelled",
                                          sent_files=0, total_files=0)
                else:
                    state = JobState.EXPIRED
                    durable_result = dict(result)
                    durable_result.update(status=state.value,
                                          reason="accepted-job-deadline")

                durable_result = _merge_delivery_summary(durable_result, summary)

                status = await connection.execute(
                    """UPDATE copy_jobs SET state = $2, stage = 'terminal',
                              next_eligible_at = NULL, lease_expires_at = NULL,
                              result = $3::jsonb,
                              updated_at = now()
                       WHERE job_id = $1 AND lease_generation = $4
                         AND lease_expires_at > clock_timestamp()""",
                    job_id, state.value,
                    json.dumps(durable_result, separators=(",", ":"), ensure_ascii=False),
                    lease_generation)
                if status != "UPDATE 1":
                    raise JobStateConflict("job lease is no longer owned")
                return state

    async def recover_interrupted(self) -> None:
        """Recover only work whose database-clock lease has expired."""
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await self._recover_expired_leases(connection)

    async def release_interrupted(self, job_id: str,
                                  lease_generation: int) -> None:
        """Resolve canceled/shutdown work without replaying possible delivery."""
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """SELECT send_intent_at, cancel_requested_at
                       FROM copy_jobs WHERE job_id = $1
                         AND state IN ('running', 'waiting_rate_limit')
                         AND lease_generation = $2
                         AND lease_expires_at > clock_timestamp()
                       FOR UPDATE""", job_id, lease_generation)
                if row is None:
                    return
                summary = await self._delivery_summary(connection, job_id)
                if row["send_intent_at"] is not None:
                    state, status, reason = _interrupted_delivery_state(summary)
                    result = {"status": status, "reason": reason}
                elif row["cancel_requested_at"] is not None:
                    state = JobState.CANCELLED
                    result = {"status": state.value,
                              "reason": "requester-cancelled",
                              "sent_files": 0, "total_files": 0}
                else:
                    state, result = JobState.QUEUED, None
                if result is not None:
                    result = _merge_delivery_summary(result, summary)
                status = await connection.execute(
                    """UPDATE copy_jobs SET state = $2, stage = $3,
                              next_eligible_at = CASE WHEN $2 = 'queued'
                                  THEN next_eligible_at ELSE NULL END,
                              lease_expires_at = NULL, result = $4::jsonb,
                              updated_at = now()
                       WHERE job_id = $1 AND state IN ('running', 'waiting_rate_limit')
                         AND lease_generation = $5
                         AND lease_expires_at > clock_timestamp()""",
                    job_id, state.value,
                    "queued" if state == JobState.QUEUED else "terminal",
                    json.dumps(result, separators=(",", ":"), ensure_ascii=False)
                    if result is not None else None, lease_generation)
                if status != "UPDATE 1":
                    raise JobStateConflict("job lease is no longer owned")
