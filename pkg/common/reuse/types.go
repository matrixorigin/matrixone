// Copyright 2023 Matrix Origin
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

package reuse

import (
	"os"
	"strings"
)

// Pool is a pool of temporary objects, designed to reduce the creation of
// temporary objects to reduce GC pressure.
//
// There are many places throughout the system where temporary objects need to
// be created, and if these places are in our hot path, we need to consider using
// a pool to reduce the number of temporary objects.
type Pool[T ReusableObject] interface {
	Alloc() *T
	Free(*T)
}

// Options options to create object pool
type Options[T ReusableObject] struct {
	release       func(*T)
	enableChecker bool
	memCapacity   int64

	// for testing
	gcRecover func()
}

// ReusableObject all reusable objects must implements this interface
type ReusableObject interface {
	// TypeName returns the name of the object type. We cannot use reflect.TypeOf to get
	// the name of the object type, to avoid mem allocate.
	// Outdated, may delete later.
	TypeName() string
}

func init() {
	spi, ok := os.LookupEnv("mo_reuse_spi")
	if ok {
		switch strings.ToLower(spi) {
		case "sync-pool":
			use(SyncBased)
		case "mpool":
			use(MpoolBased)
		}
	}

	enable, ok := os.LookupEnv("mo_reuse_enable_checker")
	if ok {
		switch strings.ToLower(enable) {
		case "true":
			enableChecker.Store(true)
		}
	}

	enable, ok = os.LookupEnv("mo_reuse_enable_checker_verbose")
	if ok {
		switch strings.ToLower(enable) {
		case "true":
			enableChecker.Store(true)
			enableVerbose.Store(true)
		}
	}
}
