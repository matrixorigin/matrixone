#!/bin/bash

# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GO_UT_ANALYSIS_VERSION="v0.0.0-20250711025253-f31acb12d3b1"

function retry_command() {
    if (( $# < 3 )); then
        echo "Usage: retry_command MAX_ATTEMPTS DELAY_SECONDS COMMAND [ARG...]" >&2
        return 2
    fi

    local max_attempts=$1
    local delay_seconds=$2
    shift 2

    if ! [[ "${max_attempts}" =~ ^[1-9][0-9]*$ ]]; then
        echo "max_attempts must be a positive integer, got '${max_attempts}'" >&2
        return 2
    fi
    if ! [[ "${delay_seconds}" =~ ^[0-9]+$ ]]; then
        echo "delay_seconds must be a non-negative integer, got '${delay_seconds}'" >&2
        return 2
    fi

    local attempt
    local status
    for (( attempt = 1; attempt <= max_attempts; attempt++ )); do
        if "$@"; then
            return 0
        else
            status=$?
        fi

        if (( attempt == max_attempts )); then
            return "${status}"
        fi
        echo "command failed (attempt ${attempt}/${max_attempts}), retrying in ${delay_seconds}s..." >&2
        sleep "${delay_seconds}"
    done
}

function install_go_ut_analysis() {
    local max_attempts=${1:-3}
    local delay_seconds=${2:-5}

    retry_command "${max_attempts}" "${delay_seconds}" \
        go install "github.com/matrixorigin/go-ut-analysis@${GO_UT_ANALYSIS_VERSION}"
}

# list_embedded_cluster_test_packages prints every requested test package whose
# race test binary transitively depends on pkg/embed. Keep this derived from the
# Go test dependency graph: a hand-maintained directory list inevitably misses
# new embedded-cluster owners and lets them compete with parallel race tests.
function list_embedded_cluster_test_packages() {
    if (( $# == 0 )); then
        echo "Usage: list_embedded_cluster_test_packages PACKAGE [PACKAGE...]" >&2
        return 2
    fi

    local embed_package
    local discovered_packages
    local template
    local -a tags_args=()

    if [[ -n "${TAGS:-}" ]]; then
        tags_args=(-tags "${TAGS}")
    fi
    # Package discovery does not consume build metadata.  Disable VCS
    # stamping so this also works from source archives and temporary module
    # fixtures that do not have a .git directory (Go 1.24+ otherwise makes
    # `go list -test` fail while trying to obtain VCS status).
    local -a build_args=(-buildvcs=false)
    embed_package=$(go list ${GO_MODULE_MODE:-} "${build_args[@]}" "${tags_args[@]}" ./pkg/embed) || return $?

    template='{{ if eq .ImportPath "'"${embed_package}"'" }}{{ .ImportPath }}{{ "\n" }}{{ end }}{{ $owner := .ForTest }}{{ range .Deps }}{{ if eq . "'"${embed_package}"'" }}{{ $owner }}{{ "\n" }}{{ end }}{{ end }}'
    discovered_packages=$(go list ${GO_MODULE_MODE:-} "${build_args[@]}" -race -test \
        "${tags_args[@]}" -f "${template}" "$@") || return $?

    printf '%s\n' "${discovered_packages}" | sed '/^$/d' | LC_ALL=C sort -u
}

# list_ut_shard_stages is the single source of truth for the race-UT shard
# contract. Keep the all-suite path explicit so a newly introduced stage cannot
# run only in UT_SHARD=all while being silently absent from every CI shard.
function list_ut_shard_stages() {
    if (( $# != 1 )); then
        echo "Usage: list_ut_shard_stages SHARD" >&2
        return 2
    fi

    case "$1" in
        all)
            printf '%s\n' light hnsw serial embedded heavy plan
            ;;
        light)
            printf '%s\n' light hnsw
            ;;
        issues)
            printf '%s\n' serial
            ;;
        embedded)
            printf '%s\n' embedded
            ;;
        heavy-plan)
            printf '%s\n' heavy plan
            ;;
        *)
            echo "Unknown UT shard '$1'" >&2
            return 2
            ;;
    esac
}

function should_run_ut_stage() {
    if (( $# != 1 )); then
        echo "Usage: should_run_ut_stage STAGE" >&2
        return 2
    fi

    if ! list_ut_shard_stages all | grep -Fxq "$1"; then
        echo "Unknown UT stage '$1'" >&2
        return 2
    fi

    list_ut_shard_stages "${UT_SHARD:-all}" | grep -Fxq "$1"
}

# validate_complete_partition proves that the supplied groups are a disjoint,
# complete partition of the authoritative item scope. It is cheap enough to run
# before every shard and makes routing or discovery drift fail closed instead of
# producing a green run with missing or duplicated coverage.
function validate_complete_partition() {
    if (( $# < 3 )); then
        echo "Usage: validate_complete_partition LABEL EXPECTED GROUP [GROUP...]" >&2
        return 2
    fi

    local label=$1
    local expected=$2
    shift 2
    local group
    local package

    {
        while IFS= read -r package; do
            if [[ -n "${package}" ]]; then
                printf 'expected\t%s\n' "${package}"
            fi
        done <<< "${expected}"

        for group in "$@"; do
            while IFS= read -r package; do
                if [[ -n "${package}" ]]; then
                    printf 'actual\t%s\n' "${package}"
                fi
            done <<< "${group}"
        done
    } | awk -F '\t' -v label="${label}" '
        $1 == "expected" { expected[$2] = 1; next }
        $1 == "actual" { actual[$2]++; next }
        END {
            failed = 0
            for (package in expected) {
                if (!(package in actual)) {
                    print "Missing " label " from partition: " package > "/dev/stderr"
                    failed = 1
                }
            }
            for (package in actual) {
                if (!(package in expected)) {
                    print "Unexpected " label " in partition: " package > "/dev/stderr"
                    failed = 1
                }
                if (actual[package] != 1) {
                    print label " occurs " actual[package] " times in partition: " package > "/dev/stderr"
                    failed = 1
                }
            }
            exit failed
        }
    '
}
