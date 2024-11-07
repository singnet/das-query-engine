#!/bin/bash

if [ $1 == "--no-destroy" ]; then
  docker stop das-query-engine-das-alpine-1
else
  # Faster test reruns
  docker compose -f compose_das_alpine.yaml down
fi

