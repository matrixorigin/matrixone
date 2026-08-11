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

package substrait

import (
	"context"
	"crypto/rand"
	"crypto/sha256"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
)

// Keep the journal namespace separate from catalog table IDs and the existing
// synthetic tables used by user-level locks (1<<62) and table-dump install
// locks (1<<62 + 1). The row is a digest of the FileService name and journal
// prefix, so independent journals do not serialize each other.
const journalAdmissionLockTableID uint64 = 1<<62 + 2

type lockServiceJournalAdmission struct {
	service lockservice.LockService
}

func newLockServiceJournalAdmission(service lockservice.LockService) (journalAdmissionCoordinator, error) {
	if service == nil {
		return nil, moerr.NewInternalErrorNoCtx("substrait: lease journal lock service is not configured")
	}
	if _, ok := service.(lockservice.ExclusiveLockService); !ok {
		return nil, moerr.NewInternalErrorNoCtx("substrait: lease journal lock service lacks fenced callbacks")
	}
	return &lockServiceJournalAdmission{service: service}, nil
}

func (c *lockServiceJournalAdmission) RunExclusive(
	ctx context.Context,
	key string,
	fn func(context.Context) error,
) error {
	if c == nil || c.service == nil || key == "" || fn == nil {
		return moerr.NewInternalErrorNoCtx("substrait: invalid journal admission callback")
	}
	row := journalAdmissionLockRow(key)
	txnID := make([]byte, sha256.Size)
	if _, err := rand.Read(txnID); err != nil {
		return moerr.NewInternalErrorNoCtxf("substrait: create journal admission identity: %v", err)
	}
	return lockservice.RunExclusiveLock(ctx, c.service, journalAdmissionLockTableID, row[:], txnID, fn)
}

func journalAdmissionLockRow(key string) [sha256.Size]byte {
	return sha256.Sum256([]byte("matrixone/substrait/journal-admission/v1\x00" + key))
}
