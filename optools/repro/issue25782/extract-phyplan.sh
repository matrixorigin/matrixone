#!/usr/bin/env bash
# Extract HashBuild runtime counters from EXPLAIN PHYPLAN ANALYZE output.
#
# The output is deliberately written to a temporary file first.  A malformed
# HashBuild must never leave a partially useful evidence file behind.
set -euo pipefail

usage() {
    printf 'usage: %s SOURCE IS_SHUFFLE DEST\n' "$0" >&2
    exit 2
}

(( $# == 3 )) || usage
source_file=$1
is_shuffle_arg=$2
dest=$3

case "$is_shuffle_arg" in
    true|1) is_shuffle=true ;;
    false|0) is_shuffle=false ;;
    *)
        printf '%s: IS_SHUFFLE must be true, false, 1, or 0\n' "$0" >&2
        # DEST is still removed below, as requested for every invocation.
        rm -f -- "$dest"
        exit 2
        ;;
esac

# Never preserve stale evidence from a previous attempt.
rm -f -- "$dest"
if [[ ! -f "$source_file" ]]; then
    printf '%s: source does not exist: %s\n' "$0" "$source_file" >&2
    exit 1
fi

dest_dir=${dest%/*}
if [[ "$dest_dir" == "$dest" ]]; then
    dest_dir=.
fi
dest_base=${dest##*/}
tmp=$(mktemp "${dest_dir}/.${dest_base}.tmp.XXXXXX")
cleanup() {
    rm -f -- "$tmp"
}
trap cleanup EXIT HUP INT TERM

if awk -v is_shuffle="$is_shuffle" '
    BEGIN {
        attempt_no = 1
        marker_expected = 1
    }

    # Return a field value and set the corresponding presence/validity flag.
    # InRows and SpillRows are plain decimal counters.  SpillSize may carry a
    # textual unit (for example, "bytes" or "B"), which is discarded.
    function field(text, label, kind,    p, rest, token, value) {
        p = index(text, label ":")
        if (p == 0) {
            return ""
        }
        rest = substr(text, p + length(label) + 1)
        sub(/^[[:space:]]*/, "", rest)
        token = rest
        sub(/[[:space:]].*$/, "", token)
        sub(/[,;)].*$/, "", token)
        if (kind == "size") {
            if (token !~ /^[0-9]+([[:alpha:]]+)?$/) {
                return "!INVALID!"
            }
        } else if (token !~ /^[0-9]+$/) {
            return "!INVALID!"
        }
        value = token
        sub(/[^0-9].*$/, "", value)
        return value
    }

    function finish(    id) {
        if (!pending) {
            return
        }
        if (cn == "" || scope_no == 0 ||
            inrows == "" || spillrows == "" || spillsize == "" ||
            inrows == "!INVALID!" || spillrows == "!INVALID!" ||
            spillsize == "!INVALID!") {
            bad = 1
        } else {
            id = "attempt-" attempt_no "-" cn "-scope-" scope_no "-hashbuild-" hb_no
            printf "%s\t%s\t%s\t%s\t%s\t%s\n", id, cn, inrows,
                is_shuffle, spillrows, spillsize
            found++
        }
        pending = 0
        inrows = ""
        spillrows = ""
        spillsize = ""
    }

    {
        if ($0 ~ /^=== MO_25782_ATTEMPT [1-9][0-9]* ===$/) {
            finish()
            marker = $0
            sub(/^=== MO_25782_ATTEMPT /, "", marker)
            sub(/ ===$/, "", marker)
            if ((marker + 0) != marker_expected) {
                bad = 1
            }
            attempt_no = marker + 0
            marker_expected = attempt_no + 1
            marker_seen++
            for (key in scope_count) delete scope_count[key]
            cn = ""
            scope_no = 0
            hb_no = 0
            next
        }

        # Only actual, line-anchored Scope headers change the CN context.  A
        # printed Scope number is not globally unique, hence scope_no is an
        # encounter ordinal maintained independently for each CN address.
        if ($0 ~ /^[[:space:]]*Scope[[:space:]]+[0-9]+([[:space:]]|$)/) {
            finish()
            line = $0
            addr = ""
            p = index(line, "addr:")
            if (p > 0) {
                addr = substr(line, p + 5)
                sub(/[,[:space:]].*$/, "", addr)
                sub(/[)].*$/, "", addr)
            }
            cn = addr
            if (cn != "") {
                scope_count[cn]++
                scope_no = scope_count[cn]
            } else {
                # Do not accidentally associate a following HashBuild with a
                # prior scope when this header has no usable addr.
                scope_no = 0
            }
            hb_no = 0
            next
        }

        lower = tolower($0)
        if (lower ~ /hash[[:space:]_-]*build/ && $0 ~ /CallNum[[:space:]]*:/) {
            # Encountering another HashBuild closes the previous one.  An
            # incomplete previous instance sets bad and prevents publication.
            finish()
            pending = 1
            hb_no++
            inrows = field($0, "InRows", "rows")
            spillrows = field($0, "SpillRows", "rows")
            spillsize = field($0, "SpillSize", "size")
            if (inrows != "" && spillrows != "" && spillsize != "") {
                finish()
            }
            next
        }

        # Counters are commonly printed on lines following the Pipeline line.
        # Keep collecting them until the next HashBuild, Scope, or EOF.
        if (pending) {
            value = field($0, "InRows", "rows")
            if (value != "") inrows = value
            value = field($0, "SpillRows", "rows")
            if (value != "") spillrows = value
            value = field($0, "SpillSize", "size")
            if (value != "") spillsize = value
        }
    }

    END {
        finish()
        if (bad || found == 0 || (marker_seen && marker_seen != 2)) {
            exit 1
        }
    }
' "$source_file" >"$tmp"; then
    mv -f -- "$tmp" "$dest"
    trap - EXIT HUP INT TERM
else
    # Keep DEST absent after any malformed/incomplete extraction.
    rm -f -- "$tmp" "$dest"
    trap - EXIT HUP INT TERM
    exit 1
fi
