#!/bin/sh
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Verify the final image, not the builder or an exported Pixi prefix.
# Usage: verify-runtime-image.sh <podman|docker> <local-image-tag>
set -eu

if [ "$#" -ne 2 ]; then
    echo "usage: $0 <container-engine> <local-image-tag>" >&2
    exit 2
fi

engine=$1
image=$2
"$engine" image inspect "$image" >/dev/null
"$engine" run --rm -i --network none --entrypoint /bin/sh "$image" -s <<'INSIDE_IMAGE'
set -eu

fail() {
    echo "GPU runtime image: $*" >&2
    exit 1
}

[ -f /mo-service ] || fail "missing mo-service"
[ -f /mocl_kernel64.fatbin ] || fail "missing CUDA fatbin"
[ -f /usr/local/lib/libmo.so ] || fail "missing libmo.so"
[ -d /usr/local/share/jieba ] || fail "missing jieba dictionary"
[ "${MO_JIEBA_DICT_DIR:-}" = /usr/local/share/jieba ] || fail "wrong jieba dictionary path"
[ ! -e /opt/pixi ] || fail "Pixi build prefix leaked into runtime"
[ ! -e /matrixone/optools/gpu/.pixi ] || fail "Pixi environment leaked into runtime"

if find -L /usr/local/lib -type l -print -quit | grep -q .; then
    fail "dangling runtime-library symlink"
fi
if find /usr/local/lib -mindepth 1 \( -name 'libcuda.so*' -o -name 'libnvidia-*' -o -name stubs \) -print -quit | grep -q .; then
    fail "packaged NVIDIA driver library or linker stub"
fi

checked=0
for library in /mo-service /usr/local/lib/*.so*; do
    [ -f "$library" ] || continue
    # Some development libraries use a linker script with a .so suffix.
    magic=$(od -An -tx1 -N4 "$library" | tr -d ' \n')
    [ "$magic" = 7f454c46 ] || continue
    checked=$((checked + 1))

    if ! dependencies=$(ldd "$library" 2>&1); then
        printf '%s\n' "$dependencies" >&2
        fail "dynamic loader rejected $library"
    fi
    case "$dependencies" in
        */opt/pixi/* | */.pixi/*) fail "Pixi build path resolves for $library" ;;
    esac
    if ! printf '%s\n' "$dependencies" | awk '
        $2 == "=>" && $3 == "not" && $4 == "found" {
            if ($1 != "libcuda.so.1" && $1 != "libnvidia-ml.so.1") {
                print "unresolved user-space dependency: " $0 > "/dev/stderr"
                bad = 1
            }
        }
        END { exit bad }
    '; then
        fail "incomplete user-space dependency closure for $library"
    fi
done

[ "$checked" -gt 1 ] || fail "no packaged ELF libraries were checked"
echo "GPU runtime image: verified $checked ELF files without the Pixi build prefix"
INSIDE_IMAGE
