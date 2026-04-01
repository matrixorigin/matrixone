// Copyright 2021-2024 Matrix Origin
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

package disttae

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func newForeignTxnTableDelegateForTest() *txnTableDelegate {
	tbl := &txnTableDelegate{
		origin: newTxnTableForTest(),
	}
	tbl.origin.relKind = catalog.SystemForeignRel
	tbl.isLocal = func() (bool, error) {
		return true, nil
	}
	return tbl
}

func TestForeignTableRangesNotSupported(t *testing.T) {
	_, err := newForeignTxnTableDelegateForTest().Ranges(context.Background(), engine.DefaultRangesParam)
	if err == nil {
		t.Fatalf("expected foreign table ranges to fail until federated reader exists")
	}
	if !strings.Contains(err.Error(), "federated foreign table") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestForeignTableBuildReadersNotSupported(t *testing.T) {
	_, err := newForeignTxnTableDelegateForTest().BuildReaders(
		context.Background(),
		nil, nil, nil,
		0, 0,
		false, 0, engine.FilterHint{},
	)
	if err == nil {
		t.Fatalf("expected foreign table reader construction to fail until federated reader exists")
	}
	if !strings.Contains(err.Error(), "federated foreign table") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestForeignTableBuildShardingReadersNotSupported(t *testing.T) {
	_, err := newForeignTxnTableDelegateForTest().BuildShardingReaders(
		context.Background(),
		nil, nil, nil,
		0, 0,
		false, 0,
	)
	if err == nil {
		t.Fatalf("expected foreign table sharding reader construction to fail until federated reader exists")
	}
	if !strings.Contains(err.Error(), "federated foreign table") {
		t.Fatalf("unexpected error: %v", err)
	}
}
