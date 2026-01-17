#!/bin/sh
set -e

# Ensure we can read the shared config during runtime
# When Railway deploys from apps/worker, the repo root isn't present.
# So we keep a copy of the config in the worker folder.
if [ -f "./config/sources.yaml" ]; then
  echo "Config already present in worker ./config"
else
  echo "Config missing in worker ./config"
fi

node index.js
