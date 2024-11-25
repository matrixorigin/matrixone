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

package fileservice

import (
	"context"
	"io"
	"iter"
	"time"
)

type ObjectStorage interface {
	// List lists objects with specified prefix
	List(
		ctx context.Context,
		prefix string,
	) iter.Seq2[*DirEntry, error]

	// Stat returns informations about an object
	Stat(
		ctx context.Context,
		key string,
	) (
		size int64,
		err error,
	)

	// Exists reports whether specified object exists
	Exists(
		ctx context.Context,
		key string,
	) (
		bool,
		error,
	)

	// Write writes an object
	Write(
		ctx context.Context,
		key string,
		r io.Reader,
		sizeHint *int64,
		expire *time.Time,
	) (
		err error,
	)

	// Read returns an io.Reader for specified object range
	Read(
		ctx context.Context,
		key string,
		min *int64,
		max *int64,
	) (
		r io.ReadCloser,
		err error,
	)

	// Delete deletes objects
	Delete(
		ctx context.Context,
		keys ...string,
	) (
		err error,
	)
}
