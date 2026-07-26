#!/usr/bin/env bash

# Run one of the three time-balanced BVT groups. The groups were derived from
# successful CI reports: pessimistic_transaction is the long group; snapshot,
# optimizer, fulltext and git4data form the second; the remaining cases form
# the third. New top-level directories are assigned deterministically.
set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 <mo-tester-dir> <case-root> <group-id>" >&2
    exit 2
fi

tester_dir=$(cd "$1" && pwd)
case_root=$(cd "$2" && pwd)
group_id=$3

if ! [[ "${group_id}" =~ ^[0-2]$ ]]; then
    echo "BVT group must be 0, 1, or 2; got '${group_id}'" >&2
    exit 2
fi

declare -a included_scripts=()
while IFS= read -r script_path; do
    relative_path=${script_path#"${case_root}/"}
    [[ "${relative_path}" == *optimistic* ]] && continue
    top_level=${relative_path%%/*}

    case "${top_level}" in
        pessimistic_transaction) assigned_group=0 ;;
        snapshot|optimizer|fulltext|git4data) assigned_group=1 ;;
        *)
            # Group 2 is the complete complement of the two explicitly
            # measured heavy groups. A new directory is covered immediately
            # here and becomes a signal to refresh the timing balance.
            assigned_group=2
            ;;
    esac

    if (( assigned_group == group_id )); then
        included_scripts+=("${script_path}")
    fi
done < <(find "${case_root}" -type f \( -name '*.sql' -o -name '*.test' \) -print | LC_ALL=C sort)

if (( ${#included_scripts[@]} == 0 )); then
    echo "BVT group ${group_id} has no test scripts" >&2
    exit 1
fi

include_list=$(IFS=,; echo "${included_scripts[*]}")
echo "Run BVT group ${group_id}: ${#included_scripts[@]} scripts"
printf '%s\n' "${included_scripts[@]}"

cd "${tester_dir}"
./run.sh -n -g -o -p "${case_root}" -i "${include_list}"
