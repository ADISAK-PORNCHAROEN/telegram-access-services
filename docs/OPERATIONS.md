# Operations Guide

This guide covers a single Docker Compose installation. Keep the Compose
project name (`telegram-access-services`) stable so Docker reuses the same
named volumes. Store backups outside the installation folder and protect them
as credentials: the app-state archive contains the owner Telegram session.

## Logs and health

The Compose services use Docker's `json-file` log driver with a 10 MiB limit
and three rotated files per container. Read logs through Docker rather than
editing or deleting Docker's internal log files:

```sh
docker compose ps
docker compose logs --tail=200 app
docker compose logs --tail=200 migrate
docker compose logs --tail=200 database
```

Application audit/performance files live in the persistent app-state volume.
Keep free disk space available for that volume and for PostgreSQL. Do not
publish full environment dumps or setup URLs in support requests.

## Back up the installation

Run from the folder containing `docker-compose.yml`. Use a private backup
directory on storage with enough free space. Stop the app first so no job or
session state changes while the database and app volume are captured:

```sh
mkdir -p backups
chmod 700 backups
docker compose stop app
docker compose exec -T database pg_dump -U telegram_service -d telegram_service -Fc > backups/database.dump
APP_VOLUME="$(docker volume ls -q \
  --filter label=com.docker.compose.project=telegram-access-services \
  --filter label=com.docker.compose.volume=customer-app-data | head -n 1)"
test -n "$APP_VOLUME"
docker run --rm \
  -v "$APP_VOLUME:/data:ro" \
  -v "$PWD/backups:/backup" \
  busybox:1.37.0 \
  sh -c 'tar -czf /backup/app-state.tar.gz --exclude=tmp -C /data .'
cp -p .env backups/env.backup
docker compose start app
```

The PostgreSQL dump contains durable jobs and delivery attempts. The app-state
archive contains the Telegram session, user settings, and application logs.
The temporary media volume is intentionally omitted. The `.env` backup contains
Telegram and database credentials; restrict access to it and never commit it.
Verify all three files exist and are non-empty before moving them to secure
backup storage. Record the Compose project name and package version alongside
the backup, without recording secret values.

## Restore a backup

Restore only into the intended installation. Check the Compose project name and
volume label before running the app-state restore: it replaces the contents of
the selected app-state volume. Keep the app stopped for the whole restore.

```sh
cp -p backups/env.backup .env
docker compose stop app
docker compose up -d database
docker compose exec -T database dropdb -U telegram_service --if-exists telegram_service
docker compose exec -T database createdb -U telegram_service -O telegram_service telegram_service
docker compose exec -T database pg_restore --no-owner -U telegram_service -d telegram_service < backups/database.dump
APP_VOLUME="$(docker volume ls -q \
  --filter label=com.docker.compose.project=telegram-access-services \
  --filter label=com.docker.compose.volume=customer-app-data | head -n 1)"
test -n "$APP_VOLUME"
docker run --rm \
  -v "$APP_VOLUME:/data" \
  -v "$PWD/backups:/backup:ro" \
  busybox:1.37.0 \
  sh -c 'find /data -mindepth 1 -maxdepth 1 -exec rm -rf -- {} + && tar -xzf /backup/app-state.tar.gz -C /data'
docker compose run --rm migrate
docker compose up -d app
```

Check the migration and app logs, then confirm expected job/attempt counts and
that the owner session is available. If the session has been revoked, complete
the one-time browser setup again. Never restore over an installation before a
current backup has been made.

## Upgrade safely

1. Create and verify a backup using the steps above.
2. Stop the app before replacing customer files. Keep `.env` and the Compose
   project name; do not delete the named volumes.
3. Build and start the new package:

   ```sh
   docker compose up -d --build
   ```

   Compose waits for PostgreSQL health and a successful migration before it
   starts the app. If migration fails, the app remains blocked; review the
   sanitized `migrate` logs and restore a known-good backup if needed.
4. Confirm `docker compose ps` shows the app running and review the logs.

Never use `docker compose down -v` during an upgrade. It removes persistent
database and app-state volumes.

## Roll back

Keep the prior package and local app image available until the new release has
been verified. Before upgrading, capture the current app image ID and tag it:

```sh
PREVIOUS_IMAGE="$(docker compose images -q app | head -n 1)"
test -n "$PREVIOUS_IMAGE"
docker image tag "$PREVIOUS_IMAGE" telegram-access-services:previous
```

For a prior application image that is known to support the current migration
ledger and schema, create a temporary `compose.rollback.yml` beside the main
Compose file:

```yaml
services:
  migrate:
    image: telegram-access-services:previous
    pull_policy: never
  app:
    image: telegram-access-services:previous
    pull_policy: never
```

Then run `docker compose -f docker-compose.yml -f compose.rollback.yml up -d`
without `--build`. Confirm the migration check succeeds before relying on the
older app. Do not assume every older release understands a newer migration
ledger.

If the previous image rejects the current schema, or its schema compatibility
is unknown, do not force startup and do not downgrade the schema manually.
Stop the app, restore both the PostgreSQL dump and app-state archive from the
same pre-upgrade backup, restore the matching `.env`, and start the previously
backed-up package/image. This is a full backup restore, not an automatic schema
rollback.
