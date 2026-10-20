import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from hashlib import sha256
from importlib.resources import files
import re


MIGRATION_ADVISORY_LOCK_KEY = 9_184_612_371
_MIGRATION_VERSION = re.compile(r"^\d{3}_[a-z0-9_]+\.sql$")
_LEDGER_TABLE = "app_schema_migrations"
_TRY_LOCK_SQL = "SELECT pg_try_advisory_lock($1::bigint)"
_UNLOCK_SQL = "SELECT pg_advisory_unlock($1::bigint)"
MIGRATION_LOCK_WAIT_SECONDS = 60.0
MIGRATION_LOCK_QUERY_TIMEOUT_SECONDS = 5.0
MIGRATION_LOCK_POLL_INTERVAL_SECONDS = 0.1

_REQUIRED_COLUMNS = {
    "copy_jobs": {
        "job_id": "text",
        "receiving_bot_id": "int8",
        "requester_id": "int8",
        "destination_chat": "int8",
        "request_message_id": "int8",
        "link_index": "int4",
        "source_chat": "text",
        "source_message_id": "int8",
        "source_is_private": "bool",
        "filter_kind": "text",
        "state": "text",
        "stage": "text",
        "send_intent_at": "timestamptz",
        "result": "jsonb",
        "created_at": "timestamptz",
        "updated_at": "timestamptz",
        "cancel_requested_at": "timestamptz",
        "next_eligible_at": "timestamptz",
        "deadline_at": "timestamptz",
        "claimed_at": "timestamptz",
        "lease_generation": "int8",
        "lease_expires_at": "timestamptz",
        "rate_limit_kind": "text",
        "rate_limit_account": "text",
        "rate_limit_required_seconds": "float8",
    },
    "copy_delivery_attempts": {
        "attempt_id": "text",
        "job_id": "text",
        "intent": "jsonb",
        "state": "text",
        "outcomes": "jsonb",
        "created_at": "timestamptz",
        "resolved_at": "timestamptz",
    },
}

_REQUIRED_CONSTRAINTS = {
    ("copy_jobs", "copy_jobs_pkey", "p"),
    ("copy_jobs", "copy_jobs_link_index_check", "c"),
    ("copy_jobs", "copy_jobs_filter_kind_check", "c"),
    ("copy_jobs", "copy_jobs_state_check", "c"),
    ("copy_jobs", "copy_jobs_stage_check", "c"),
    ("copy_jobs", "copy_jobs_rate_limit_kind_check", "c"),
    ("copy_jobs", "copy_jobs_rate_limit_account_check", "c"),
    ("copy_jobs", "copy_jobs_rate_limit_required_seconds_check", "c"),
    ("copy_delivery_attempts", "copy_delivery_attempts_pkey", "p"),
    ("copy_delivery_attempts", "copy_delivery_attempts_job_id_fkey", "f"),
    ("copy_delivery_attempts", "copy_delivery_attempts_state_check", "c"),
}
_REQUIRED_NOT_NULL_COLUMNS = {
    ("copy_jobs", column)
    for column in (
        "job_id", "receiving_bot_id", "requester_id", "destination_chat",
        "request_message_id", "link_index", "source_chat", "source_message_id",
        "source_is_private", "state", "stage", "created_at", "updated_at",
        "deadline_at", "lease_generation",
    )
} | {
    ("copy_delivery_attempts", column)
    for column in (
        "attempt_id", "job_id", "intent", "state", "outcomes", "created_at",
    )
}


@dataclass(frozen=True, slots=True)
class Migration:
    version: str
    checksum: str
    sql: str


@dataclass(frozen=True, slots=True)
class MigrationResult:
    applied: tuple[str, ...]
    already_applied: tuple[str, ...]


class MigrationError(RuntimeError):
    """A safe migration failure message that contains no database details."""


async def _acquire_migration_lock(connection) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + MIGRATION_LOCK_WAIT_SECONDS
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise MigrationError("migration lock wait timed out")
        try:
            acquired = await connection.fetchval(
                _TRY_LOCK_SQL,
                MIGRATION_ADVISORY_LOCK_KEY,
                timeout=min(MIGRATION_LOCK_QUERY_TIMEOUT_SECONDS, remaining),
            )
        except asyncio.CancelledError:
            connection.terminate()
            raise
        except Exception:
            connection.terminate()
            raise MigrationError("migration lock acquisition failed") from None
        if acquired:
            return
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise MigrationError("migration lock wait timed out")
        await asyncio.sleep(min(MIGRATION_LOCK_POLL_INTERVAL_SECONDS, remaining))


async def _release_migration_lock(connection) -> None:
    try:
        released = await connection.fetchval(
            _UNLOCK_SQL,
            MIGRATION_ADVISORY_LOCK_KEY,
            timeout=MIGRATION_LOCK_QUERY_TIMEOUT_SECONDS,
        )
    except asyncio.CancelledError:
        connection.terminate()
        raise
    except Exception:
        connection.terminate()
        raise MigrationError("migration lock cleanup failed") from None
    if not released:
        connection.terminate()
        raise MigrationError("migration lock ownership was lost")


@asynccontextmanager
async def _migration_lock(connection):
    await _acquire_migration_lock(connection)
    try:
        yield
    finally:
        await _release_migration_lock(connection)


def load_migrations() -> tuple[Migration, ...]:
    package = files("app.infrastructure.storage.migrations")
    resources = sorted(
        (entry for entry in package.iterdir() if entry.name.endswith(".sql")),
        key=lambda entry: entry.name,
    )
    migrations = []
    for resource in resources:
        content = resource.read_bytes()
        if not _MIGRATION_VERSION.fullmatch(resource.name) or not content.strip():
            raise MigrationError("migration package is invalid")
        try:
            sql = content.decode("utf-8")
        except UnicodeDecodeError:
            raise MigrationError("migration package is invalid") from None
        migrations.append(Migration(
            version=resource.name,
            checksum=sha256(content).hexdigest(),
            sql=sql,
        ))
    if not migrations:
        raise MigrationError("migration package is empty")
    return tuple(migrations)


class PostgresMigrationRunner:
    def __init__(self, pool, migrations: tuple[Migration, ...] | None = None):
        self.pool = pool
        self.migrations = tuple(migrations if migrations is not None else load_migrations())
        self._validate_catalog()

    def _validate_catalog(self) -> None:
        if not self.migrations:
            raise MigrationError("migration package is empty")
        if any(not isinstance(migration, Migration) for migration in self.migrations):
            raise MigrationError("migration package is invalid")
        versions = tuple(migration.version for migration in self.migrations)
        if any(not isinstance(version, str)
               or not _MIGRATION_VERSION.fullmatch(version) for version in versions):
            raise MigrationError("migration package is invalid")
        if versions != tuple(sorted(set(versions))):
            raise MigrationError("migration package is invalid")
        numbers = tuple(int(version[:3]) for version in versions)
        if numbers != tuple(range(1, len(numbers) + 1)):
            raise MigrationError("migration package is invalid")
        for migration in self.migrations:
            if (
                not isinstance(migration.sql, str)
                or not migration.sql.strip()
                or not isinstance(migration.checksum, str)
                or not re.fullmatch(r"[0-9a-f]{64}", migration.checksum)
                or sha256(migration.sql.encode("utf-8")).hexdigest() != migration.checksum
            ):
                raise MigrationError("migration package is invalid")

    async def apply(self) -> MigrationResult:
        try:
            return await self._apply()
        except MigrationError:
            raise
        except Exception:
            raise MigrationError("database migration failed") from None

    async def _apply(self) -> MigrationResult:
        async with self.pool.acquire() as connection:
            async with _migration_lock(connection):
                await connection.execute(
                    f"""CREATE TABLE IF NOT EXISTS {_LEDGER_TABLE} (
                        version text PRIMARY KEY,
                        checksum text NOT NULL,
                        applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
                    )"""
                )
                rows = await connection.fetch(
                    f"SELECT version, checksum FROM {_LEDGER_TABLE} ORDER BY version"
                )
                recorded = self._validate_history(rows)

                if not recorded:
                    await self._preflight_legacy_schema(connection)

                applied = []
                for index, migration in enumerate(self.migrations[len(recorded):],
                                                   start=len(recorded)):
                    async with connection.transaction():
                        await connection.execute(migration.sql)
                        if index == len(self.migrations) - 1:
                            await self._validate_schema(connection)
                        await connection.execute(
                            f"""INSERT INTO {_LEDGER_TABLE} (version, checksum)
                                VALUES ($1, $2)""",
                            migration.version,
                            migration.checksum,
                        )
                    applied.append(migration.version)

                if not applied:
                    await self._validate_schema(connection)

                return MigrationResult(
                    applied=tuple(applied),
                    already_applied=tuple(migration.version
                                           for migration in self.migrations[:len(recorded)]),
                )

    async def _preflight_legacy_schema(self, connection) -> None:
        transaction = connection.transaction()
        await transaction.start()
        try:
            for migration in self.migrations:
                await connection.execute(migration.sql)
            await self._validate_schema(connection)
        finally:
            await transaction.rollback()

    def _validate_history(self, rows) -> tuple[Migration, ...]:
        by_version = {migration.version: migration for migration in self.migrations}
        records = {row["version"]: row["checksum"] for row in rows}
        if any(version not in by_version for version in records):
            raise MigrationError("migration history is incompatible")

        versions = tuple(by_version)
        recorded_versions = tuple(records)
        if recorded_versions != versions[:len(recorded_versions)]:
            raise MigrationError("migration history is incompatible")

        for version, checksum in records.items():
            if checksum != by_version[version].checksum:
                raise MigrationError("migration checksum does not match history")
        return tuple(by_version[version] for version in recorded_versions)

    async def _validate_schema(self, connection) -> None:
        rows = await connection.fetch(
            """SELECT table_name, column_name, udt_name, is_nullable
               FROM information_schema.columns
               WHERE table_schema = current_schema()
                 AND table_name = ANY($1::text[])""",
            list(_REQUIRED_COLUMNS),
        )
        actual = {(row["table_name"], row["column_name"]): row["udt_name"]
                  for row in rows}
        nullable = {(row["table_name"], row["column_name"])
                    for row in rows if row["is_nullable"] == "YES"}
        for table_name, columns in _REQUIRED_COLUMNS.items():
            for column_name, data_type in columns.items():
                if actual.get((table_name, column_name)) != data_type:
                    raise MigrationError("database schema is incompatible")
        if _REQUIRED_NOT_NULL_COLUMNS & nullable:
            raise MigrationError("database schema is incompatible")

        constraints = await connection.fetch(
            """SELECT relation.relname AS table_name,
                      constraint_row.conname AS constraint_name,
                      constraint_row.contype::text AS constraint_type,
                      constraint_row.convalidated AS is_validated,
                      pg_get_constraintdef(constraint_row.oid, true) AS definition
               FROM pg_constraint AS constraint_row
               JOIN pg_class AS relation
                 ON relation.oid = constraint_row.conrelid
               JOIN pg_namespace AS namespace_row
                 ON namespace_row.oid = relation.relnamespace
               WHERE namespace_row.nspname = current_schema()
                 AND relation.relname = ANY($1::text[])""",
            list(_REQUIRED_COLUMNS),
        )
        present = {(row["table_name"], row["constraint_name"], row["constraint_type"])
                   for row in constraints}
        if not _REQUIRED_CONSTRAINTS <= present:
            raise MigrationError("database schema is incompatible")
        if any(
            (row["table_name"], row["constraint_name"], row["constraint_type"])
            in _REQUIRED_CONSTRAINTS and not row["is_validated"]
            for row in constraints
        ):
            raise MigrationError("database schema is incompatible")
        definitions = {
            (row["table_name"], row["constraint_name"]): _normalize_definition(
                row["definition"])
            for row in constraints
        }
        required_definitions = {
            ("copy_jobs", "copy_jobs_pkey"): "primary key (job_id)",
            ("copy_jobs", "copy_jobs_link_index_check"): "check (link_index >= 0)",
            ("copy_jobs", "copy_jobs_filter_kind_check"):
                "check (filter_kind is null or filter_kind = any (array['photo', 'video', 'file']))",
            ("copy_jobs", "copy_jobs_state_check"):
                "check (state = any (array['queued', 'running', 'waiting_rate_limit', 'completed', 'partial', 'failed', 'expired', 'cancelled', 'delivery_unknown']))",
            ("copy_jobs", "copy_jobs_stage_check"):
                "check (stage = any (array['queued', 'resolve', 'download', 'upload', 'send', 'terminal']))",
            ("copy_jobs", "copy_jobs_rate_limit_kind_check"):
                "check (rate_limit_kind is null or rate_limit_kind = any (array['flood', 'premium']))",
            ("copy_jobs", "copy_jobs_rate_limit_account_check"):
                "check (rate_limit_account is null or rate_limit_account = any (array['owner', 'bot', 'unknown']))",
            ("copy_jobs", "copy_jobs_rate_limit_required_seconds_check"):
                "check (rate_limit_required_seconds is null or rate_limit_required_seconds > (0)::double precision)",
            ("copy_delivery_attempts", "copy_delivery_attempts_state_check"):
                "check (state = any (array['intent', 'confirmed', 'rejected']))",
            ("copy_delivery_attempts", "copy_delivery_attempts_job_id_fkey"):
                "foreign key (job_id) references copy_jobs(job_id) on delete cascade",
            ("copy_delivery_attempts", "copy_delivery_attempts_pkey"):
                "primary key (attempt_id)",
        }
        for identity, expected in required_definitions.items():
            if definitions.get(identity) != _normalize_definition(expected):
                raise MigrationError("database schema is incompatible")

        unique_keys = await connection.fetch(
            """SELECT relation.relname AS table_name,
                      array_agg(attribute.attname ORDER BY key_column.ordinality)
                          AS columns
               FROM pg_constraint AS constraint_row
               JOIN pg_class AS relation
                 ON relation.oid = constraint_row.conrelid
               JOIN pg_namespace AS namespace_row
                 ON namespace_row.oid = relation.relnamespace
               CROSS JOIN LATERAL unnest(constraint_row.conkey)
                   WITH ORDINALITY AS key_column(attnum, ordinality)
               JOIN pg_attribute AS attribute
                 ON attribute.attrelid = relation.oid
                AND attribute.attnum = key_column.attnum
               WHERE namespace_row.nspname = current_schema()
                 AND relation.relname = 'copy_jobs'
                 AND constraint_row.contype = 'u'
               GROUP BY relation.relname, constraint_row.oid"""
        )
        if not any(tuple(row["columns"]) == (
            "receiving_bot_id", "destination_chat", "request_message_id", "link_index"
        ) for row in unique_keys):
            raise MigrationError("database schema is incompatible")


def _normalize_definition(definition: str) -> str:
    normalized = re.sub(r"[\s()]+", "", definition.replace('"', "")).lower()
    normalized = normalized.replace("::text", "").replace("::doubleprecision", "")
    return normalized.removeprefix("check")
