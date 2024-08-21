#!/bin/bash

which metta 2>&1 > /dev/null || {
cat <<EOF
metta not found in PATH, please install hyperon package:

  pip install hyperon

EOF
exit 1
}

metta --version | grep -E '^(0\.1\.1[2-9]|0\.[2-9])' 2>&1>/dev/null || {
cat <<EOF
metta version 0.1.12 or higher is required
current installed version is $(metta --version), please upgrade:

  - using pip:
    pip install --upgrade hyperon
    check version: metta --version
  OR
  - building from source:
    clone the repository: https://github.com/trueagi-io/hyperon-experimental
    follow the instructions here: https://github.com/trueagi-io/hyperon-experimental/blob/main/README.md
    check version: metta --version

EOF
exit 1
}

metta_scripts_root="${PWD}/tests/performance/dasgate"

echo -n "Running performance test using MeTTa's built-in Atom Space..."
metta_script="${metta_scripts_root}/test_builtin.metta"
builtin_log_file="$(mktemp --suffix='_perf_test_builtin.log')"
time (metta "${metta_script}" 2>&1 > "${builtin_log_file}")
echo "Check the log file for more details: ${builtin_log_file}"

echo "-----"

echo -n "Running performance test using DAS..."
metta_script="${metta_scripts_root}/test_das_ram_only.metta"
das_log_file="$(mktemp --suffix='_perf_test_das_ram_only.log')"
time (metta "${metta_script}" 2>&1 > "${das_log_file}")
echo "Check the log file for more details: ${das_log_file}"

echo "-----"

echo -n "Comparing tests outputs... "
if diff \
    <(sed 's/, /\n/g' "${builtin_log_file}" | tr -d '[]()' | sort | xargs echo) \
    <(sed 's/, /\n/g' "${das_log_file}" | tr -d '[]()' | sort | xargs echo) \
    2>&1 > /dev/null
then
echo "SUCCESS!"
echo "Tests outputs are the same on both built-in Atom Space and DAS."
exit 0
else
echo "FAILED!"
cat <<EOF
Tests outputs are different between the built-in Atom Space and DAS.
Check the log files for more details:
  - Built-in Atom Space: ${builtin_log_file}
  - DAS: ${das_log_file}

EOF
exit 1
fi
