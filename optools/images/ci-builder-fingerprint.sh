#!/bin/sh

# Print the native-build contract shared by ci-builder producers and
# consumers.  The output is deliberately independent of the checkout path so
# it can be compared between a Docker build context and a mounted PR tree.

set -eu

root=${CI_BUILDER_SOURCE_ROOT:-$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)}
cd "$root"
# Keep tool discovery independent of the caller's login/non-login shell. The
# producer runs during docker build and consumers run in a non-login shell;
# both must probe the same system tool locations.
PATH=${CI_BUILDER_TOOL_PATH:-/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin}
export PATH

hash_files() {
	find "$@" -type f -print0 |
		LC_ALL=C sort -z |
		xargs -r -0 sha256sum |
		sha256sum |
		awk '{print $1}'
}

hash_thirdparties() {
	(
		cd thirdparties
		# Include source-side CUDA headers and every other checked-in native
		# input, while excluding generated install/build output.
		find . -type f \
			-not -path './install/*' \
			-not -path './_*' -print0 |
			LC_ALL=C sort -z |
			xargs -r -0 sha256sum
	) | sha256sum | awk '{print $1}'
}

hash_cgo() {
	find cgo -type f \
		\( -name '*.c' -o -name '*.cc' -o -name '*.cpp' -o -name '*.cu' \
		-o -name '*.h' -o -name '*.hh' -o -name '*.hpp' -o -name '*.go' \
		-o -name 'Makefile' \) -print0 |
		LC_ALL=C sort -z |
		xargs -r -0 sha256sum |
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
cmake_version=${cmake_version:-unavailable}
# Read the version of the image's local go binary without allowing a PR's
# go.mod toolchain directive to switch/download another toolchain. Native
# dependencies are produced and consumed by the same image toolchain.
go_version=$(CDPATH= cd / && GOTOOLCHAIN=local go version 2>/dev/null | awk '{print $3}' || true)
go_version=${go_version:-unavailable}

cat <<EOF
schema=3
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
