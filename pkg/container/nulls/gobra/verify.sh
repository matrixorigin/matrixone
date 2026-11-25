#!/usr/bin/env bash
# Formally verify the Nulls core (nulls.go) with Gobra.
# See gobra/README.md for what is proven and what is trusted.
# Requires: GOBRA_JAR (path to gobra.jar) and Z3_EXE (path to z3 4.16).
#
# The bitmap package is imported through its specifications only; verify it
# separately with pkg/common/bitmap/gobra/verify.sh. -o restricts Gobra to
# files carrying the `// +gobra` header, which keeps the unverified
# *_ext.go files and tests out of both packages.
set -euo pipefail
dir=$(cd "$(dirname "$0")/.." && pwd)
root=$(cd "$dir/../../.." && pwd)
: "${GOBRA_JAR:?set GOBRA_JAR to the path of gobra.jar}"
: "${Z3_EXE:?set Z3_EXE to the path of z3}"
export Z3_EXE
: "${GOBRA_JAVA_OPTS:=-Xss128m -Xmx2g}"
cd "$dir"
exec java $GOBRA_JAVA_OPTS -jar "$GOBRA_JAR" \
	-o -m github.com/matrixorigin/matrixone \
	-I "$root/pkg/common/bitmap/gobra/stubs" -I "$root" \
	-i nulls.go "$@"
