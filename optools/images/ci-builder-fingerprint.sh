#!/bin/sh

# Print the native-build contract shared by ci-builder producers and
# consumers.  The output is deliberately independent of the checkout path so
# it can be compared between a Docker build context and a mounted PR tree.

set -eu

root=${CI_BUILDER_SOURCE_ROOT:-$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)}
cd "$root"

hash_files() {
	find "$@" -type f -print0 |
		LC_ALL=C sort -z |
		xargs -0 sha256sum |
		sha256sum |
		awk '{print $1}'
}

hash_thirdparties() {
	(
		cd thirdparties
		find . -maxdepth 1 -type f -print0 |
			LC_ALL=C sort -z |
			xargs -0 sha256sum
	) | sha256sum | awk '{print $1}'
}

hash_cgo() {
	find cgo -type f \
		\( -name '*.c' -o -name '*.cc' -o -name '*.cpp' -o -name '*.cu' \
		-o -name '*.h' -o -name '*.hh' -o -name '*.hpp' -o -name '*.go' \
		-o -name 'Makefile' \) -print0 |
		LC_ALL=C sort -z |
		xargs -0 sha256sum |
		sha256sum |
		awk '{print $1}'
}

compiler_version() {
	compiler=$1
	if command -v "$compiler" >/dev/null 2>&1; then
		printf '%s-%s' \
			"$($compiler -dumpmachine 2>/dev/null || true)" \
			"$($compiler -dumpfullversion -dumpversion 2>/dev/null || true)"
	else
		printf 'unavailable'
	fi
}

cmake_version=$(cmake --version 2>/dev/null | sed -n '1p' || true)
go_version=$(go env GOVERSION 2>/dev/null || true)

cat <<EOF
schema=2
os=$(uname -s)
arch=$(uname -m)
go=${go_version}
cc=$(compiler_version cc)
cxx=$(compiler_version c++)
cmake=${cmake_version}
root_makefile=$(hash_files Makefile)
cgo=$(hash_cgo)
thirdparties=$(hash_thirdparties)
EOF
