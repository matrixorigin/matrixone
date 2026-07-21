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
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	icebergwritecore "github.com/matrixorigin/matrixone/pkg/iceberg/write"
)

type DMLCommitWorkflowStore interface {
	PublishJobInserter
	OrphanFileInserter
}

type DMLCommitWorkflowOptions struct {
	Config           api.Config
	ManifestWriter   dml.ManifestObjectWriter
	ObjectIOProvider icebergio.ObjectIOProvider
	ScopeForLocation icebergio.ObjectScopeForLocation
	Committer        api.Committer
	CommitVerifier   dml.CommitVerifier
	CacheInvalidator icebergwritecore.CacheInvalidator
	MetricsReporter  api.MetricsReporter
	Now              func() time.Time
	OrphanTTL        time.Duration
}

func NewDMLCommitWorkflow(store DMLCommitWorkflowStore, opts DMLCommitWorkflowOptions) dml.CommitWorkflow {
	manifestWriter := opts.ManifestWriter
	if manifestWriter == nil && opts.ObjectIOProvider != nil {
		manifestWriter = icebergio.ProviderObjectWriter{
			Provider:         opts.ObjectIOProvider,
			ScopeForLocation: opts.ScopeForLocation,
		}
	}
	metricsReporter := opts.MetricsReporter
	if metricsReporter == nil {
		if reporter, ok := opts.Committer.(api.MetricsReporter); ok {
			metricsReporter = reporter
		}
	}
	commitVerifier := opts.CommitVerifier
	if commitVerifier == nil {
		if client, ok := opts.Committer.(api.CatalogClient); ok {
			commitVerifier = dml.CatalogCommitVerifier{Client: client}
		}
	}
	orphanTTL := opts.OrphanTTL
	if orphanTTL <= 0 {
		orphanTTL = opts.Config.Write.OrphanTTL
	}
	return dml.CommitWorkflow{
		ManifestWriter:   manifestWriter,
		Committer:        opts.Committer,
		Verifier:         commitVerifier,
		OrphanRecorder:   OrphanFileRecorder{DAO: store},
		AuditRecorder:    PublishAuditRecorder{DAO: store},
		CacheInvalidator: opts.CacheInvalidator,
		MetricsReporter:  metricsReporter,
		Now:              opts.Now,
		OrphanTTL:        orphanTTL,
	}
}

var _ DMLCommitWorkflowStore = (*DAO)(nil)
