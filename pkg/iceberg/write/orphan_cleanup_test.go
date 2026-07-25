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

package write

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

func TestFileServiceOrphanCleanerDeletesResolvedObject(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-cleanup", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: "orders/data/part-1.parquet",
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 3, ReaderForWrite: bytes.NewReader([]byte("abc"))}},
	})
	require.NoError(t, err)
	cleaner := FileServiceOrphanCleaner{
		Resolver: FileServiceResolverFunc(func(ctx context.Context, location string) (fileservice.FileService, string, error) {
			require.Equal(t, "s3://warehouse/orders/data/part-1.parquet", location)
			return fs, "orders/data/part-1.parquet", nil
		}),
		ReferenceChecker: OrphanReferenceCheckerFunc(func(ctx context.Context, candidate OrphanCandidate) (bool, error) {
			require.Equal(t, "s3://warehouse/orders/data/part-1.parquet", candidate.FilePath)
			return false, nil
		}),
		StatementPrefix: "s3://warehouse/orders/data/",
	}
	err = cleaner.CleanupOrphan(ctx, OrphanCandidate{FilePath: "s3://warehouse/orders/data/part-1.parquet"})
	require.NoError(t, err)
	_, err = fs.StatFile(ctx, "orders/data/part-1.parquet")
	require.Error(t, err)
}

func TestFileServiceOrphanCleanerRefusesReferencedObject(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-cleanup-referenced", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: "orders/data/part-1.parquet",
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 3, ReaderForWrite: bytes.NewReader([]byte("abc"))}},
	})
	require.NoError(t, err)
	cleaner := FileServiceOrphanCleaner{
		Resolver: FileServiceResolverFunc(func(ctx context.Context, location string) (fileservice.FileService, string, error) {
			return fs, "orders/data/part-1.parquet", nil
		}),
		ReferenceChecker: OrphanReferenceCheckerFunc(func(ctx context.Context, candidate OrphanCandidate) (bool, error) {
			return true, nil
		}),
		StatementPrefix: "s3://warehouse/orders/data/",
	}
	err = cleaner.CleanupOrphan(ctx, OrphanCandidate{FilePath: "s3://warehouse/orders/data/part-1.parquet"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "referenced file")
	_, err = fs.StatFile(ctx, "orders/data/part-1.parquet")
	require.NoError(t, err)
}

func TestFileServiceOrphanCleanerRequiresSafetyGates(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("iceberg-cleanup-gated", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	err = fs.Write(ctx, fileservice.IOVector{
		FilePath: "orders/data/part-1.parquet",
		Entries:  []fileservice.IOEntry{{Offset: 0, Size: 3, ReaderForWrite: bytes.NewReader([]byte("abc"))}},
	})
	require.NoError(t, err)
	resolver := FileServiceResolverFunc(func(ctx context.Context, location string) (fileservice.FileService, string, error) {
		return fs, "orders/data/part-1.parquet", nil
	})

	err = (FileServiceOrphanCleaner{
		Resolver:        resolver,
		StatementPrefix: "s3://warehouse/orders/data/",
	}).CleanupOrphan(ctx, OrphanCandidate{FilePath: "s3://warehouse/orders/data/part-1.parquet"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "reference checker")

	err = (FileServiceOrphanCleaner{
		Resolver: resolver,
		ReferenceChecker: OrphanReferenceCheckerFunc(func(ctx context.Context, candidate OrphanCandidate) (bool, error) {
			return false, nil
		}),
		StatementPrefix: "s3://warehouse/other-statement/",
	}).CleanupOrphan(ctx, OrphanCandidate{FilePath: "s3://warehouse/orders/data/part-1.parquet"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "statement prefix")

	_, err = fs.StatFile(ctx, "orders/data/part-1.parquet")
	require.NoError(t, err)
}
