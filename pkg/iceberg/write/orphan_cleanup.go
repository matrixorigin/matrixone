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
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type FileServiceResolver interface {
	ResolveFileService(ctx context.Context, location string) (fileservice.FileService, string, error)
}

type FileServiceResolverFunc func(ctx context.Context, location string) (fileservice.FileService, string, error)

func (f FileServiceResolverFunc) ResolveFileService(ctx context.Context, location string) (fileservice.FileService, string, error) {
	return f(ctx, location)
}

type OrphanReferenceChecker interface {
	IsReferenced(ctx context.Context, candidate OrphanCandidate) (bool, error)
}

type OrphanReferenceCheckerFunc func(ctx context.Context, candidate OrphanCandidate) (bool, error)

func (f OrphanReferenceCheckerFunc) IsReferenced(ctx context.Context, candidate OrphanCandidate) (bool, error) {
	return f(ctx, candidate)
}

type FileServiceOrphanCleaner struct {
	Resolver         FileServiceResolver
	ReferenceChecker OrphanReferenceChecker
	StatementPrefix  string
}

func (c FileServiceOrphanCleaner) CleanupOrphan(ctx context.Context, candidate OrphanCandidate) error {
	if c.Resolver == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg orphan cleaner requires a FileService resolver", nil))
	}
	location := strings.TrimSpace(candidate.FilePath)
	if location == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg orphan cleaner requires a file path", nil))
	}
	prefix := strings.TrimSpace(c.StatementPrefix)
	if prefix == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg orphan cleaner requires a statement-scoped prefix", map[string]string{
			"location": api.RedactPath(location),
		}))
	}
	if !strings.HasPrefix(location, prefix) {
		return api.ToMOErr(ctx, api.NewError(api.ErrOrphanCleanupFailed, "Iceberg orphan cleaner refuses to delete outside the statement prefix", map[string]string{
			"location": api.RedactPath(location),
			"prefix":   api.RedactPath(prefix),
		}))
	}
	if c.ReferenceChecker == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg orphan cleaner requires a committed metadata reference checker", map[string]string{
			"location": api.RedactPath(location),
		}))
	}
	referenced, err := c.ReferenceChecker.IsReferenced(ctx, candidate)
	if err != nil {
		return api.ToMOErr(ctx, api.WrapError(api.ErrOrphanCleanupFailed, "Iceberg orphan cleaner failed to verify committed metadata references", map[string]string{
			"location": api.RedactPath(location),
		}, err))
	}
	if referenced {
		return api.ToMOErr(ctx, api.NewError(api.ErrOrphanCleanupFailed, "Iceberg orphan cleaner refuses to delete a referenced file", map[string]string{
			"location": api.RedactPath(location),
		}))
	}
	fs, path, err := c.Resolver.ResolveFileService(ctx, location)
	if err != nil {
		return err
	}
	if fs == nil || strings.TrimSpace(path) == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg orphan cleaner resolver returned an invalid target", map[string]string{
			"location": api.RedactPath(location),
		}))
	}
	if err := fs.Delete(ctx, path); err != nil {
		return api.ToMOErr(ctx, api.WrapError(api.ErrOrphanCleanupFailed, "Iceberg orphan cleaner failed to delete object", map[string]string{
			"location": api.RedactPath(location),
		}, err))
	}
	return nil
}
