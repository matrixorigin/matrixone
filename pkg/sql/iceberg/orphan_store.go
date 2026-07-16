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

package iceberg

import (
	"context"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	icebergwrite "github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

const (
	OrphanCleanupStatusPending = "pending"
	OrphanCleanupStatusDeleted = "deleted"
	OrphanCleanupStatusFailed  = "failed"
)

type OrphanCleanupStoreDAO interface {
	ListOrphanCleanupCandidates(ctx context.Context, accountID uint32, limit int) ([]model.OrphanFile, error)
	UpdateOrphanFileCleanupStatus(ctx context.Context, accountID uint32, jobID, filePathHash, cleanupStatus string, expectedVersion uint64) error
}

type OrphanCleanupCatalogGetter interface {
	GetCatalogByID(ctx context.Context, accountID uint32, catalogID uint64) (model.Catalog, error)
}

type OrphanSweeperStore interface {
	OrphanCleanupStoreDAO
	OrphanCleanupCatalogGetter
}

type OrphanCleanupStore struct {
	DAO       OrphanCleanupStoreDAO
	AccountID uint32
}

type OrphanSweeperOptions struct {
	Store             OrphanSweeperStore
	AccountID         uint32
	CatalogFactory    maintenance.CatalogClientFactory
	ObjectIOProvider  icebergio.ObjectIOProvider
	ScopeForLocation  icebergio.ObjectScopeForLocation
	Metadata          api.MetadataFacade
	PlanningMaxMemory int64
	Now               func() time.Time
	Limit             int
}

type OrphanSweeper struct {
	opts OrphanSweeperOptions
}

func NewOrphanSweeper(opts OrphanSweeperOptions) OrphanSweeper {
	return OrphanSweeper{opts: opts}
}

func (s OrphanSweeper) Sweep(ctx context.Context) (maintenance.SweepResult, error) {
	if s.opts.Store == nil {
		return maintenance.SweepResult{}, api.NewError(api.ErrConfigInvalid, "Iceberg orphan sweeper requires a store", nil)
	}
	if s.opts.AccountID == 0 {
		return maintenance.SweepResult{}, api.NewError(api.ErrConfigInvalid, "Iceberg orphan sweeper requires account_id", nil)
	}
	if s.opts.CatalogFactory == nil {
		return maintenance.SweepResult{}, api.NewError(api.ErrConfigInvalid, "Iceberg orphan sweeper requires a catalog factory", nil)
	}
	if s.opts.ObjectIOProvider == nil {
		return maintenance.SweepResult{}, api.NewError(api.ErrConfigInvalid, "Iceberg orphan sweeper requires an object IO provider", nil)
	}
	sweep := maintenance.MarkAndSweep{
		Store: OrphanCleanupStore{
			DAO:       s.opts.Store,
			AccountID: s.opts.AccountID,
		},
		Cleaner: orphanSweeperCleaner{
			CatalogGetter:     s.opts.Store,
			CatalogFactory:    s.opts.CatalogFactory,
			ObjectIOProvider:  s.opts.ObjectIOProvider,
			ScopeForLocation:  s.opts.ScopeForLocation,
			Metadata:          s.opts.Metadata,
			PlanningMaxMemory: s.opts.PlanningMaxMemory,
		},
		Now:   s.opts.Now,
		Limit: s.opts.Limit,
	}
	return sweep.Sweep(ctx)
}

func NewOrphanMarkAndSweep(dao OrphanCleanupStoreDAO, accountID uint32, cleaner icebergwrite.OrphanCleaner, limit int) maintenance.MarkAndSweep {
	return maintenance.MarkAndSweep{
		Store:   OrphanCleanupStore{DAO: dao, AccountID: accountID},
		Cleaner: cleaner,
		Limit:   limit,
	}
}

func (s OrphanCleanupStore) ListExpiredOrphans(ctx context.Context, _ time.Time, limit int) ([]icebergwrite.OrphanCandidate, error) {
	if s.DAO == nil {
		return nil, moerr.NewInvalidInput(ctx, "iceberg orphan cleanup store requires a DAO")
	}
	if s.AccountID == 0 {
		return nil, moerr.NewInvalidInput(ctx, "iceberg orphan cleanup store requires account_id")
	}
	files, err := s.DAO.ListOrphanCleanupCandidates(ctx, s.AccountID, limit)
	if err != nil {
		return nil, err
	}
	out := make([]icebergwrite.OrphanCandidate, 0, len(files))
	for _, file := range files {
		out = append(out, orphanFileToCandidate(file))
	}
	return out, nil
}

func (s OrphanCleanupStore) MarkOrphanDeleted(ctx context.Context, candidate icebergwrite.OrphanCandidate) error {
	return s.updateStatus(ctx, candidate, OrphanCleanupStatusDeleted)
}

func (s OrphanCleanupStore) MarkOrphanRetry(ctx context.Context, candidate icebergwrite.OrphanCandidate, _ string) error {
	return s.updateStatus(ctx, candidate, OrphanCleanupStatusPending)
}

func (s OrphanCleanupStore) MarkOrphanFailed(ctx context.Context, candidate icebergwrite.OrphanCandidate, _ string) error {
	return s.updateStatus(ctx, candidate, OrphanCleanupStatusFailed)
}

func (s OrphanCleanupStore) updateStatus(ctx context.Context, candidate icebergwrite.OrphanCandidate, status string) error {
	if s.DAO == nil {
		return moerr.NewInvalidInput(ctx, "iceberg orphan cleanup store requires a DAO")
	}
	if candidate.AccountID == 0 || candidate.JobID == "" || candidate.FilePathHash == "" || candidate.Version == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg orphan cleanup status update requires account_id, job_id, file_path_hash, and version")
	}
	return s.DAO.UpdateOrphanFileCleanupStatus(ctx, candidate.AccountID, candidate.JobID, candidate.FilePathHash, status, candidate.Version)
}

func orphanFileToCandidate(file model.OrphanFile) icebergwrite.OrphanCandidate {
	return icebergwrite.OrphanCandidate{
		AccountID:         file.AccountID,
		CatalogID:         file.CatalogID,
		JobID:             file.JobID,
		Namespace:         file.Namespace,
		TableName:         file.TableName,
		TableLocationHash: file.TableLocationHash,
		FilePath:          file.FilePath,
		FilePathHash:      file.FilePathHash,
		FilePathRedacted:  file.FilePathRedacted,
		WrittenAt:         file.WrittenAt,
		ExpireAt:          file.ExpireAt,
		CleanupStatus:     file.CleanupStatus,
		Version:           file.Version,
	}
}

type orphanSweeperCleaner struct {
	CatalogGetter     OrphanCleanupCatalogGetter
	CatalogFactory    maintenance.CatalogClientFactory
	ObjectIOProvider  icebergio.ObjectIOProvider
	ScopeForLocation  icebergio.ObjectScopeForLocation
	Metadata          api.MetadataFacade
	PlanningMaxMemory int64
}

func (c orphanSweeperCleaner) CleanupOrphan(ctx context.Context, candidate icebergwrite.OrphanCandidate) error {
	if c.CatalogGetter == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg orphan sweeper cleaner requires a catalog getter", nil))
	}
	if c.CatalogFactory == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg orphan sweeper cleaner requires a catalog factory", nil))
	}
	if c.ObjectIOProvider == nil {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg orphan sweeper cleaner requires an object IO provider", nil))
	}
	if candidate.AccountID == 0 || candidate.CatalogID == 0 {
		return api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg orphan sweeper candidate requires account_id and catalog_id", nil))
	}
	if strings.TrimSpace(candidate.FilePath) == "" {
		return api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg orphan sweeper candidate requires file path", nil))
	}
	catalog, err := c.CatalogGetter.GetCatalogByID(ctx, candidate.AccountID, candidate.CatalogID)
	if err != nil {
		return err
	}
	client, err := c.CatalogFactory.NewClient(ctx, catalog)
	if err != nil {
		return err
	}
	scopeForLocation := orphanSweeperScopeForCandidate(candidate, c.ScopeForLocation)
	reader := icebergio.ProviderObjectReader{
		Provider:         c.ObjectIOProvider,
		ScopeForLocation: scopeForLocation,
	}
	checker := maintenance.MetadataReferenceChecker{
		Loader: maintenance.CatalogMetadataLoader{
			Client:  client,
			Catalog: api.CatalogRequest{Catalog: catalog},
		},
		Metadata:       c.Metadata,
		ObjectReader:   reader,
		MaxMemoryBytes: c.PlanningMaxMemory,
	}
	cleaner := icebergwrite.FileServiceOrphanCleaner{
		Resolver: objectIOFileServiceResolver{
			Provider:         c.ObjectIOProvider,
			ScopeForLocation: scopeForLocation,
		},
		ReferenceChecker: checker,
		// Use the exact recorded object path as the deletion boundary. This is
		// narrower than a directory sweep and keeps cleanup scoped to audited rows.
		StatementPrefix: strings.TrimSpace(candidate.FilePath),
	}
	return cleaner.CleanupOrphan(ctx, candidate)
}

func orphanSweeperScopeForCandidate(candidate icebergwrite.OrphanCandidate, base icebergio.ObjectScopeForLocation) icebergio.ObjectScopeForLocation {
	return func(location string) icebergio.ObjectScope {
		scope := icebergio.ObjectScope{StorageLocation: strings.TrimSpace(location)}
		if base != nil {
			scope = base(location)
			if strings.TrimSpace(scope.StorageLocation) == "" {
				scope.StorageLocation = strings.TrimSpace(location)
			}
		}
		if scope.AccountID == 0 {
			scope.AccountID = candidate.AccountID
		}
		if scope.CatalogID == 0 {
			scope.CatalogID = candidate.CatalogID
		}
		if strings.TrimSpace(scope.Principal) == "" {
			scope.Principal = "orphan-sweeper"
		}
		return scope
	}
}

type objectIOFileServiceResolver struct {
	Provider         icebergio.ObjectIOProvider
	ScopeForLocation icebergio.ObjectScopeForLocation
}

func (r objectIOFileServiceResolver) ResolveFileService(ctx context.Context, location string) (fileservice.FileService, string, error) {
	if r.Provider == nil {
		return nil, "", api.ToMOErr(ctx, api.NewError(api.ErrConfigInvalid, "Iceberg orphan object resolver requires ObjectIOProvider", nil))
	}
	scope := icebergio.ObjectScope{StorageLocation: strings.TrimSpace(location)}
	if r.ScopeForLocation != nil {
		scope = r.ScopeForLocation(location)
		if strings.TrimSpace(scope.StorageLocation) == "" {
			scope.StorageLocation = strings.TrimSpace(location)
		}
	}
	return r.Provider.Resolve(ctx, scope)
}

var _ maintenance.OrphanStore = OrphanCleanupStore{}
var _ OrphanCleanupCatalogGetter = (*DAO)(nil)
