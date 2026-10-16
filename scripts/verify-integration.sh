#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
docker info >/dev/null
# Fails early without Docker instead of silently treating skipped container tests as verification.
mvn clean verify "$@"
