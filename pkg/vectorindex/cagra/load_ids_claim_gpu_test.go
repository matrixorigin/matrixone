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

//go:build gpu

package cagra

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	vimemory "github.com/matrixorigin/matrixone/pkg/vectorindex/memory"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// claimSpy records what LoadIndex asks the host governor for. Swapping the
// package's reserveHostMemory is the only way to see the claim at all: the
// alternative -- filling the real ledger to within the fixture's ~2 KB of ids --
// races live availability, which moves by hundreds of KB between measuring the
// budget and reserving against it.
type claimSpy struct {
	calls int
	bytes uint64
	who   string
	fail  error
}

func (c *claimSpy) install(t *testing.T) {
	t.Helper()
	orig := reserveHostMemory
	t.Cleanup(func() { reserveHostMemory = orig })
	reserveHostMemory = func(b uint64, who string) (*vimemory.HostReservation, error) {
		c.calls++
		c.bytes, c.who = b, who
		if c.fail != nil {
			return nil, c.fail
		}
		return orig(b, who)
	}
}

// loadForClaimTest drives a real LoadIndex against a local artifact, with the
// tag=1/tag=2 SELECTs stubbed out. Mirrors loadedModel in search_test.go, but
// returns the error instead of asserting success -- these cases are about the
// failure paths.
func loadForClaimTest(t *testing.T, id, path, checksum string, size int64) (*CagraModel[float32, float32], error) {
	t.Helper()
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)

	orig := runSql
	t.Cleanup(func() { runSql = orig })
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: proc.Mp()}, nil
	}

	loader := &CagraModel[float32, float32]{
		Id: id, Path: path, Checksum: checksum, FileSize: size, Devices: []int{0},
	}
	return loader, loader.LoadIndex(sqlproc, testIdxcfg(), testTblcfg(), 1, false)
}

// LoadIndex claims host memory for the ids array Unpack materialises. The claim
// must be sized from the ARTIFACT, never from idxcfg.IndexCapacity.
//
// Regression: it was sized from IndexCapacity, which is zero on every search-path
// load -- it is resolved by the build operator and never written back to
// algo_params, and ParamsFromTree persists max_index_capacity only when the user
// supplied a positive value. The guarded claim therefore never fired in
// production, and an 88M artifact grew ~704 MB of ids outside the governor.
//
// The first version of this test only checked ids.bin's length, so the broken
// implementation -- which claimed nothing at all -- would have passed it too.
// These cases execute LoadIndex and observe the reservation itself.
func TestLoadIndexClaimsIdsSizedFromArtifact(t *testing.T) {
	ids := make([]int64, testNVectors)
	for i := range ids {
		ids[i] = int64(i + 5000)
	}
	built := buildTestModel(t, "ids-claim", ids)
	tarPath := built.Path
	t.Cleanup(func() { os.Remove(tarPath) })

	sizes, err := cuvs.MeasureTar(tarPath)
	require.NoError(t, err)
	idsBytes := sizes.Files["ids.bin"]
	require.Positive(t, idsBytes, "an id-bearing artifact must carry ids.bin")
	// save_ids writes a uint64 count header ahead of the array and load_ids reads
	// it back the same way, so the file is 8 + rows*sizeof(int64). The claim
	// over-states host_ids by that header, which is the direction that cannot
	// under-admit.
	require.Equal(t, int64(8+len(ids)*8), idsBytes,
		"ids.bin is a uint64 count header plus rows*sizeof(int64)")

	t.Run("claims exactly the artifact's ids and releases on success", func(t *testing.T) {
		spy := &claimSpy{}
		spy.install(t)

		loader, lerr := loadForClaimTest(t, "ids-claim", tarPath, built.Checksum, built.FileSize)
		require.NoError(t, lerr)
		require.NotNil(t, loader.Index)
		t.Cleanup(func() { loader.Index.Destroy() })

		// The whole point of the fix: a claim is actually taken, and its size comes
		// from the artifact. Zero calls is what the IndexCapacity version did.
		require.Equal(t, 1, spy.calls, "exactly one host claim per load")
		require.Equal(t, uint64(idsBytes), spy.bytes,
			"the claim must be sized from ids.bin, not from idxcfg.IndexCapacity")
		require.Equal(t, "cagra load ids", spy.who)

		// Settled once host_ids is materialised. Holding it past the allocation
		// would double-count the same bytes against every later build.
		require.Zero(t, vimemory.HostReservedBytes(),
			"the claim must be released once Unpack has materialised host_ids")
	})

	t.Run("a refused claim fails the load and strands nothing", func(t *testing.T) {
		spy := &claimSpy{fail: moerr.NewInternalErrorNoCtx("host budget exhausted")}
		spy.install(t)

		loader, lerr := loadForClaimTest(t, "ids-claim", tarPath, built.Checksum, built.FileSize)
		require.Error(t, lerr, "a refused host claim must fail the load, not proceed unadmitted")
		require.ErrorContains(t, lerr, "host budget exhausted")

		// Refusing after the GPU handle exists must still release it -- otherwise
		// every refused load orphans VRAM for the process lifetime.
		require.Nil(t, loader.Index, "the GPU handle must be destroyed on a refused claim")
		require.Equal(t, 1, spy.calls)
		require.Zero(t, vimemory.HostReservedBytes(), "a refusal must leave the ledger empty")
	})

	t.Run("an unreadable artifact fails before any claim is taken", func(t *testing.T) {
		// Checksum is computed from the file, so a corrupt artifact with a matching
		// checksum gets past that guard and is refused deeper in -- before MeasureTar
		// and before the claim. What matters is that nothing was reserved.
		bad := filepathJoinTemp(t, "corrupt.tar")
		require.NoError(t, os.WriteFile(bad, []byte("not a tar at all"), 0o600))
		sum, cerr := vectorindex.CheckSum(bad)
		require.NoError(t, cerr)

		spy := &claimSpy{}
		spy.install(t)

		_, lerr := loadForClaimTest(t, "corrupt", bad, sum, 16)
		require.Error(t, lerr, "a corrupt artifact must not load")
		require.Zero(t, spy.calls, "no host claim may be taken for an artifact that cannot be read")
		require.Zero(t, vimemory.HostReservedBytes())
	})
}

func filepathJoinTemp(t *testing.T, name string) string {
	t.Helper()
	return t.TempDir() + "/" + name
}
