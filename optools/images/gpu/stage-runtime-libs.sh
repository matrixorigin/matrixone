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

# Flatten the two Pixi runtime library roots into one image directory. Pixi's
# top-level lib/ contains symlinks into targets/x86_64-linux/lib, including a
# libcudart symlink whose basename collides with the real target file. Stage
# each resolved file once, use hard links for its aliases, and reject different
# libraries that would claim the same runtime basename.
set -eu

if [ "$#" -ne 2 ]; then
    echo "usage: $0 <pixi-prefix> <empty-runtime-directory>" >&2
    exit 2
fi

prefix=$(realpath "$1")
destination=$2
cuda_lib=$prefix/targets/x86_64-linux/lib
if [ ! -d "$prefix/lib" ] || [ ! -d "$cuda_lib" ]; then
    echo "incomplete Pixi runtime library roots: $prefix" >&2
    exit 1
fi
mkdir -p "$destination"
if [ -n "$(find "$destination" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    echo "GPU runtime staging directory must be empty: $destination" >&2
    exit 1
fi

staged=0
for source in "$prefix"/lib/*.so* "$cuda_lib"/*.so*; do
    if [ ! -e "$source" ] && [ ! -L "$source" ]; then
        continue
    fi
    if [ ! -f "$source" ]; then
        echo "invalid or dangling Pixi runtime library: $source" >&2
        exit 1
    fi
    resolved=$(realpath "$source")
    case "$resolved" in
        "$prefix"/*) ;;
        *) echo "Pixi runtime library escapes its prefix: $source" >&2; exit 1 ;;
    esac
    case "$resolved" in
        */stubs/*) echo "driver stub in Pixi runtime library: $source" >&2; exit 1 ;;
    esac

    name=${source##*/}
    resolved_name=${resolved##*/}
    for candidate in "$name" "$resolved_name"; do
        case "$candidate" in
            libcuda.so* | libnvidia-*)
                echo "host driver library in Pixi runtime staging: $source" >&2
                exit 1
                ;;
        esac
    done

    canonical=$destination/$resolved_name
    if [ -e "$canonical" ] || [ -L "$canonical" ]; then
        if ! cmp -s "$resolved" "$canonical"; then
            echo "conflicting Pixi runtime library basename: $resolved_name" >&2
            exit 1
        fi
    else
        cp -p "$resolved" "$canonical"
    fi

    alias=$destination/$name
    if [ "$alias" != "$canonical" ]; then
        if [ -e "$alias" ] || [ -L "$alias" ]; then
            if ! cmp -s "$resolved" "$alias"; then
                echo "conflicting Pixi runtime library basename: $name" >&2
                exit 1
            fi
        else
            ln "$canonical" "$alias"
        fi
    fi
    staged=$((staged + 1))
done

if [ "$staged" -eq 0 ]; then
    echo "no Pixi runtime libraries found: $prefix" >&2
    exit 1
fi
