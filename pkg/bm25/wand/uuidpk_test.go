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

package wand

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const uuidPkType = int32(types.T_uuid)

// TestWandUuidPkEncodeReps: a uuid pk arrives as a CDC STRING (extractRowFromVector
// -> Uuid.String()) OR as a sync-build types.Uuid (GetAny). Both MUST encode to the
// SAME stored bytes and decode back to types.Uuid (what AppendAny + the doc_id ->
// src.id INNER JOIN need). This is exactly the representation the earlier generic
// codec crashed on.
func TestWandUuidPkEncodeReps(t *testing.T) {
	utext := "0195e0c8-1234-7890-abcd-000000000001"
	u, err := types.ParseUuid(utext)
	if err != nil {
		t.Fatal(err)
	}

	bStr, err := encodePk(uuidPkType, utext) // CDC representation (string)
	if err != nil {
		t.Fatal(err)
	}
	bVal, err := encodePk(uuidPkType, u) // sync representation (types.Uuid)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bStr, bVal) {
		t.Fatalf("uuid encode differs by rep: string=%q value=%q", bStr, bVal)
	}

	got, err := decodePk(uuidPkType, bStr)
	if err != nil {
		t.Fatal(err)
	}
	gu, ok := got.(types.Uuid)
	if !ok {
		t.Fatalf("decodePk returned %T, want types.Uuid", got)
	}
	if gu != u {
		t.Fatalf("decodePk uuid mismatch: got %v want %v", gu, u)
	}
	// normalizeKey must agree so a string-delivered delete matches a Uuid segment pk.
	if normalizeKey(got) != normalizeKey(u) {
		t.Fatal("normalizeKey(decoded) != normalizeKey(types.Uuid)")
	}
}

// TestWandUuidPkRoundTrip builds a segment whose pks are delivered as STRINGS (the
// CDC form), serializes + deserializes it, and confirms search returns the correct
// docs with types.Uuid doc-ids — the full path that must not panic. Also checks a
// string-delivered delete folds to the same key as the reloaded Uuid pk (liveness).
func TestWandUuidPkRoundTrip(t *testing.T) {
	u1s := "00000000-0000-0000-0000-000000000001"
	u2s := "00000000-0000-0000-0000-000000000002"
	u1, _ := types.ParseUuid(u1s)
	u2, _ := types.ParseUuid(u2s)

	b := NewBuilder("uuidtest", uuidPkType)
	adds := []struct {
		w  string
		pk any
	}{{"x", u1s}, {"x", u2s}, {"y", u1s}} // string pks, the CDC representation
	for _, a := range adds {
		if err := b.Add(a.w, a.pk); err != nil {
			t.Fatal(err)
		}
	}
	m := b.Finish()

	blob, err := m.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	m2, err := Deserialize("uuidtest", bytes.NewReader(blob))
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Free()
	if m2.N != 2 {
		t.Fatalf("N=%d want 2", m2.N)
	}

	res := SearchSegments([]*WandModel{m2}, []string{"x"}, 10, nil)
	if len(res) != 2 {
		t.Fatalf("search x: %d results want 2", len(res))
	}
	got := map[types.Uuid]bool{}
	for _, r := range res {
		u, ok := r.DocID.(types.Uuid)
		if !ok {
			t.Fatalf("DocID is %T, want types.Uuid (needed for AppendAny + src join)", r.DocID)
		}
		got[u] = true
	}
	if !got[u1] || !got[u2] {
		t.Fatalf("search returned wrong uuids: %v", got)
	}

	// A DELETE delivered as the CDC string must fold to the SAME key as the reloaded
	// types.Uuid segment pk, or liveness would never match it.
	dl, err := EncodeDeleteLog(uuidPkType, []DeleteRecord{{Pk: u1s}})
	if err != nil {
		t.Fatal(err)
	}
	drecs, err := DecodeDeleteLog(dl)
	if err != nil {
		t.Fatal(err)
	}
	dm := FoldDeleteFrame(nil, drecs, 3)
	if dm[normalizeKey(u1)] != 3 {
		t.Fatalf("string-delivered uuid delete did not fold to the types.Uuid key: %v", dm)
	}
}
