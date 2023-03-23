// Copyright 2022 Matrix Origin
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

import "context"

// MutableFileService is an extension interface to FileService that allow mutation
type MutableFileService interface {
	FileService

	// NewMutator creates a new mutator
	NewMutator(ctx context.Context, filePath string) (Mutator, error)
}

type Mutator interface {

	// Mutate mutates file contents
	Mutate(ctx context.Context, entries ...IOEntry) error

	// Append appends data to file
	// all IOEntry.Offset is base on the end of file position
	// for example, passing IOEntry{Offset: 0, Len:1, Data: []byte("a")} will append "a" to the end of file
	Append(ctx context.Context, entries ...IOEntry) error

	// Close closes the mutator
	// Must be called after finishing mutation
	Close() error
}
