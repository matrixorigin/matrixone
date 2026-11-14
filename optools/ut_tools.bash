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

# Find test packages whose race-enabled dependency graph includes pkg/embed.
# Looking at test dependencies catches clusters started through test helpers.
function list_embedded_cluster_test_packages() {
    if (( $# == 0 )); then
        echo "Usage: list_embedded_cluster_test_packages PACKAGE [PACKAGE...]" >&2
        return 2
    fi

    local embed_package discovered_packages template
    local -a tags_args=()
    if [[ -n "${TAGS:-}" ]]; then
        tags_args=(-tags "${TAGS}")
    fi
    embed_package=$(go list ${GO_MODULE_MODE:-} "${tags_args[@]}" ./pkg/embed) || return $?
    template='{{ if eq .ImportPath "'"${embed_package}"'" }}{{ .ImportPath }}{{ "\n" }}{{ end }}{{ $owner := .ForTest }}{{ range .Deps }}{{ if eq . "'"${embed_package}"'" }}{{ $owner }}{{ "\n" }}{{ end }}{{ end }}'
    discovered_packages=$(go list ${GO_MODULE_MODE:-} -race -test \
        "${tags_args[@]}" -f "${template}" "$@") || return $?
    printf '%s\n' "${discovered_packages}" | sed '/^$/d' | LC_ALL=C sort -u
}

# The requested scope must be partitioned exactly once; never silently drop a
# package or introduce one outside the scope while building the race phases.
function validate_ut_package_partition() {
    local scope=$1 package group
    shift
    local -A requested=() seen=()
    if [[ -z "${scope//[[:space:]]/}" ]]; then
        echo "UT package scope is empty" >&2
        return 1
    fi
    for package in ${scope}; do
        if [[ -n "${requested[${package}]:-}" ]]; then
            echo "Duplicate requested UT package: ${package}" >&2
            return 1
        fi
        requested[${package}]=1
    done
    for group in "$@"; do
        for package in ${group}; do
            if [[ -z "${requested[${package}]:-}" ]]; then
                echo "UT package outside requested scope: ${package}" >&2
                return 1
            fi
            if [[ -n "${seen[${package}]:-}" ]]; then
                echo "UT package assigned more than once: ${package}" >&2
                return 1
            fi
            seen[${package}]=1
        done
    done
    for package in ${scope}; do
        if [[ -z "${seen[${package}]:-}" ]]; then
            echo "UT package missing from phases: ${package}" >&2
            return 1
        fi
    done
}
