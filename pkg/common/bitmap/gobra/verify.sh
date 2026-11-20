#!/usr/bin/env bash
# Formally verify the bitmap core (bitmap.go) with Gobra.
# See gobra/README.md for what is proven and what is trusted.
# Requires: GOBRA_JAR (path to gobra.jar) and Z3_EXE (path to z3 4.16).
set -euo pipefail
dir=$(cd "$(dirname "$0")/.." && pwd)
: "${GOBRA_JAR:?set GOBRA_JAR to the path of gobra.jar}"
: "${Z3_EXE:?set Z3_EXE to the path of z3}"
export Z3_EXE
cd "$dir"
exec java -Xss128m -jar "$GOBRA_JAR" \
	-I gobra/stubs \
	-i types.go bitmap.go bitmap_lemmas.gobra "$@"
