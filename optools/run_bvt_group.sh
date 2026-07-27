#!/usr/bin/env bash

# Run one of the two time-balanced BVT groups. The latest BVT timing assigns
# about 1501s to group 0 and 1479s to group 1. Keep the statement-info
# producers and verifiers in one group: mo-tester runs directories in lexical order, so log,
# result_count, sql_source_type, and statement_query_type run before
# zz_statement_query_type.
#
# New top-level directories are assigned deterministically so every case is
# exercised immediately; rebalance the explicit list from collected timing.
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
    echo "Usage: $0 <mo-tester-dir> <case-root> <group-id> [resource-dir]" >&2
    exit 2
fi

tester_dir=$(cd "$1" && pwd)
case_root=$(cd "$2" && pwd)
group_id=$3
resource_dir=${4:-}

if ! [[ "${group_id}" =~ ^[01]$ ]]; then
    echo "BVT group must be 0 or 1; got '${group_id}'" >&2
    exit 2
fi

declare -a included_scripts=()
declare -a resource_args=()
if [[ -n "${resource_dir}" ]]; then
    resource_dir=$(cd "${resource_dir}" && pwd)
    resource_args=(-s "${resource_dir}")
fi

while IFS= read -r script_path; do
    relative_path=${script_path#"${case_root}/"}
    [[ "${relative_path}" == *optimistic* ]] && continue
    top_level=${relative_path%%/*}

    case "${top_level}" in
        array|auto_increment|benchmark|dataXtest|ddl|disttae|fake_pk|function|git4data|hint|join|keyword|load_data|mo_cloud|optimizer|pg_cast|plugin|prepare|procedure|query_result|sample|save_query_result|sequence|snapshot|sql_inject|stage|system|system_variable|temporary|tenant|tenxcloud_xx|time_window|union|util|vector|view)
            assigned_group=0
            ;;
        analyze|charset_collation|comment|cte|database|distinct|dml|dtype|expression|feature_limit|foreign_key|fulltext|geo|iceberg|log|metadata|operator|pessimistic_transaction|pitr|plan_cache|publication_subscription|qexec|recursive_cte|replace_statement|result_count|security|set|sql_source_type|statement_query_type|subquery|table|task|udf|window|zz_accesscontrol|zz_statement_query_type)
            assigned_group=1
            ;;
        *)
            # Do not defer newly added suites: deterministically place them in
            # one group and rebalance the explicit list from collected timing.
            assigned_group=$(( $(printf '%s' "${top_level}" | cksum | awk '{print $1}') % 2 ))
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
if [[ -n "${resource_dir}" ]]; then
    echo "BVT resource directory: ${resource_dir}"
fi
printf '%s\n' "${included_scripts[@]}"

cd "${tester_dir}"
./run.sh -n -g -o -p "${case_root}" -i "${include_list}" "${resource_args[@]}"
