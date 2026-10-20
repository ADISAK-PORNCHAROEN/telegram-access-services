#!/bin/sh
set -eu
umask 077

# The application roots state beside bot.py. Refresh only this immutable launcher
# on each start; application packages stay outside the persistent /data volume.
cp /opt/service/bot.py /data/bot.py
exec "$@"
