#!/bin/bash

output=$(make performance-tests 2>&1)
real_time=$(echo "$output" | grep "^real" | awk 'NR==2 {print $2}')

if echo "$output" | grep -q 'SUCCESS!'; then
  status="SUCCESS!"
else
  status="FAILURE!"
fi

echo "$real_time $status"

if [ "$status" == "SUCCESS!" ]; then
  exit 0
else
  exit 1
fi
