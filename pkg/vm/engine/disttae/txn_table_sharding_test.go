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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestStatsIsLocalErr(t *testing.T) {
	runIsLocalErrTests(
		t,
		func(tbl *txnTableDelegate) {
			_, err := tbl.Stats(context.Background(), false)
			require.Error(t, err)
		},
	)
}

func TestRangesIsLocalErr(t *testing.T) {
	runIsLocalErrTests(
		t,
		func(tbl *txnTableDelegate) {
			_, err := tbl.Ranges(context.Background(), nil, 0)
			require.Error(t, err)
		},
	)
}

func TestCollectTombstonesIsLocalErr(t *testing.T) {
	runIsLocalErrTests(
		t,
		func(tbl *txnTableDelegate) {
			_, err := tbl.CollectTombstones(context.Background(), 0, 0)
			require.Error(t, err)
		},
	)
}

func TestGetColumMetadataScanInfoIsLocalErr(t *testing.T) {
	runIsLocalErrTests(
		t,
		func(tbl *txnTableDelegate) {
			_, err := tbl.GetColumMetadataScanInfo(context.Background(), "")
			require.Error(t, err)
		},
	)
}

func TestBuildReadersIsLocalErr(t *testing.T) {
	runIsLocalErrTests(
		t,
		func(tbl *txnTableDelegate) {
			_, err := tbl.BuildReaders(context.Background(), nil, nil, nil, 0, 0, false, 0)
			require.Error(t, err)
		},
	)
}

func TestMergeObjectsIsLocalErr(t *testing.T) {
	runIsLocalErrTests(
		t,
		func(tbl *txnTableDelegate) {
			_, err := tbl.MergeObjects(context.Background(), nil, 0)
			require.Error(t, err)
		},
	)
}

func TestGetNonAppendableObjectStatsIsLocalErr(t *testing.T) {
	runIsLocalErrTests(
		t,
		func(tbl *txnTableDelegate) {
			_, err := tbl.GetNonAppendableObjectStats(context.Background())
			require.Error(t, err)
		},
	)
}

func runIsLocalErrTests(
	_ *testing.T,
	fn func(tbl *txnTableDelegate),
) {
	tbl := &txnTableDelegate{}
	tbl.isLocal = func() (bool, error) {
		return false, moerr.NewNotSupportedNoCtx("")
	}

	fn(tbl)
}
