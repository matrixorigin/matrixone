#!/bin/bash
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

set -eu
if (( $# == 0 )); then
    echo 'ut_link_gate: missing Go tool' >&2
    exit 2
fi
# Tool identity queries MUST be byte-for-byte transparent to preserve Go's
# cache keys. Do not take a slot for compiler/assembler/cgo/version invocations.
if [[ "${1##*/}" != link || "${2:-}" == '-V=full' ]]; then
    exec "$@"
fi
if ! [[ "${UT_LINK_PARALLEL:-}" =~ ^[1-9][0-9]?$ ]] ||
    (( UT_LINK_PARALLEL > 64 )) || [[ ! -d "${UT_LINK_DIR:-}" ]]; then
    echo 'ut_link_gate: invalid slot count or directory' >&2
    exit 2
fi
started=${SECONDS}
# Rotate the first probe to avoid always concentrating traffic on slot zero.
first=$(( $$ % UT_LINK_PARALLEL ))
reported_wait=0
while :; do
    for (( offset=0; offset<UT_LINK_PARALLEL; offset++ )); do
        slot=$(( (first + offset) % UT_LINK_PARALLEL ))
        exec 9<>"${UT_LINK_DIR}/slot-${slot}"
        if flock -n 9; then
            if [[ -n "${UT_LINK_LOG:-}" ]]; then
                printf 'event=acquired pid=%s slot=%s wait_seconds=%s\n' "$$" "${slot}" "$(( SECONDS - started ))" >> "${UT_LINK_LOG}" || true
            fi
            # No supervising shell remains. FD 9 carries the kernel lease into
            # the linker; normal exit, errors and signals all release it.
            exec "$@"
        else
            status=$?
            exec 9>&-
            # Only contention is retryable; filesystem/locking errors fail closed.
            if (( status != 1 )); then exit "${status}"; fi
        fi
    done
    if (( reported_wait == 0 )) && [[ -n "${UT_LINK_LOG:-}" ]]; then
        printf 'event=waiting pid=%s\n' "$$" >> "${UT_LINK_LOG}" || true
        reported_wait=1
    fi
    sleep 0.05
done
