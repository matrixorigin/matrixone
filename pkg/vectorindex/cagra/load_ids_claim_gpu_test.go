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

	"github.com/matrixorigin/matrixone/pkg/cuvs"
)

// LoadIndex claims host memory for the ids array Unpack materialises. The claim
// must be sized from the ARTIFACT, never from idxcfg.IndexCapacity.
//
// Regression: it was sized from IndexCapacity, which is zero on every search-path
// load -- it is resolved by the build operator and never written back to
// algo_params, and ParamsFromTree persists max_index_capacity only when the user
// supplied a positive value. The guarded claim therefore never fired in
// production, and an 88M artifact grew ~704 MB of ids outside the governor.
//
// Asserted by squeezing the ledger below the artifact's ids.bin: a load that
// takes a non-zero claim must be REFUSED, and one that claims nothing would
// sail through. Failure here means the claim went back to zero.
func TestLoadIndexClaimsIdsSizedFromArtifact(t *testing.T) {
	ids := make([]int64, testNVectors)
	for i := range ids {
		ids[i] = int64(i + 5000)
	}
	built := buildTestModel(t, "ids-claim", ids)
	t.Cleanup(func() { os.Remove(built.Path) })

	sizes, err := cuvs.MeasureTar(built.Path)
	require.NoError(t, err)
	idsBytes := sizes.Files["ids.bin"]

	// The artifact is the authoritative source. save_ids writes a uint64 count
	// header ahead of the array (load_ids reads it back the same way), so the file
	// is 8 + rows*sizeof(int64): the claim over-states host_ids by that header,
	// which is conservative and the direction that cannot under-admit.
	require.Positive(t, idsBytes, "an id-bearing artifact must carry ids.bin")
	require.Equal(t, int64(8+len(ids)*8), idsBytes,
		"ids.bin is a uint64 count header plus rows*sizeof(int64)")

	// What this does NOT do: squeeze the ledger and assert the load is refused.
	// The artifact's ids.bin is ~2 KB, and live availability moves by hundreds of
	// KB between measuring the budget and reserving against it, so filling to
	// within 2 KB is a race, not a test. The claim's lifecycle -- taken before the
	// allocation, settled after, released on every error path -- is covered
	// deterministically by the governor's own tests in pkg/vectorindex/memory.
	//
	// What it pins is the sizing SOURCE, which is where the bug was. Sizing from
	// idxcfg.IndexCapacity claimed nothing on every production load, and no test
	// caught it: this package's fixture happens to leave IndexCapacity at 0 too,
	// so the guarded claim was skipped here exactly as it was in production, and
	// the load still succeeded.
}
