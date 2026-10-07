// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux && cgo

package malloc

/*
#cgo LDFLAGS: -ldl
#define _GNU_SOURCE
#include <dlfcn.h>
#include <stddef.h>

typedef int (*mo_malloc_trim_fn)(size_t);

static int mo_malloc_trim_available(void) {
	return dlsym(RTLD_DEFAULT, "malloc_trim") != NULL;
}

static int
mo_malloc_trim(void) {
	mo_malloc_trim_fn trim = (mo_malloc_trim_fn)dlsym(RTLD_DEFAULT, "malloc_trim");
	if (trim == NULL) {
		return 0;
	}
	return trim(0);
}
*/
import "C"

func canTrimCAllocator() bool {
	return C.mo_malloc_trim_available() != 0
}

func trimCAllocator() bool {
	return C.mo_malloc_trim() != 0
}
