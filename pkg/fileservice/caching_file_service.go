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

// CachingFileService is an extension to the FileService
type CachingFileService interface {
	FileService

	// FlushCache flushes cache
	FlushCache()

	// SetAsyncUpdate sets cache update operation to async mode
	SetAsyncUpdate(bool)

	// CacheStats returns cache statistics
	CacheStats() *CacheStats
}

type CacheStats struct {
	NumRead int64
	NumHit  int64
}
