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

package engine

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	issue26465DepartmentIndexes   = 768
	issue26465DepartmentRefChilds = 349
	issue26465DepartmentBytes     = 133622

	issue26465UserIndexes   = 732
	issue26465UserFKeys     = 349
	issue26465UserRefChilds = 480
	issue26465UserBytes     = 180356

	issue26465CandidateIndexes = 3
	issue26465CandidateFKeys   = 702
	issue26465CandidateBytes   = 146242

	// A constraint update serializes the complete definition in
	// UpdateConstraint, AlterTable, and txnDatabase.createWithID.
	issue26465MarshalsPerConstraintUpdate = 3
)

var issue26465BenchmarkSink int

type issue26465QAConstraintFixture struct {
	departments *ConstraintDef
	users       *ConstraintDef
	candidates  *ConstraintDef
}

func TestIssue26465QAConstraintFixture(t *testing.T) {
	fixture := newIssue26465QAConstraintFixture()

	assertIssue26465ConstraintShape(
		t,
		fixture.departments,
		issue26465DepartmentIndexes,
		0,
		issue26465DepartmentRefChilds,
		issue26465DepartmentBytes,
	)
	assertIssue26465ConstraintShape(
		t,
		fixture.users,
		issue26465UserIndexes,
		issue26465UserFKeys,
		issue26465UserRefChilds,
		issue26465UserBytes,
	)
	assertIssue26465ConstraintShape(
		t,
		fixture.candidates,
		issue26465CandidateIndexes,
		issue26465CandidateFKeys,
		0,
		issue26465CandidateBytes,
	)
}

// BenchmarkIssue26465QAConstraintRewrite models the allocation hot path from
// QA's 2026-08-07 allocation profile. It deliberately uses the decoded QA
// cardinalities and constraint-blob sizes, while keeping the fixture in-memory
// and deterministic. The observed-loop cases describe QA's legacy fan-out;
// the bounded cases are the acceptance target for the reconciliation fix.
//
// Run manually with:
// go test -run '^$' -bench '^BenchmarkIssue26465QAConstraintRewrite$' -benchmem -benchtime=1x ./pkg/vm/engine
func BenchmarkIssue26465QAConstraintRewrite(b *testing.B) {
	fixture := newIssue26465QAConstraintFixture()

	// A no-op COPY ALTER on departments visits 349 ref-child entries. QA had
	// one current users table and 348 historical entries; users is used as the
	// measured current constraint size for each rewrite.
	benchmarkIssue26465ConstraintRewrite(
		b,
		"departments_copy/observed_349_users_rewrites",
		fixture.users,
		issue26465DepartmentRefChilds,
	)
	benchmarkIssue26465ConstraintRewrite(
		b,
		"departments_copy/bounded_one_users_rewrite",
		fixture.users,
		1,
	)

	// A COPY ALTER on users visits each of its 349 FKs even though every FK
	// references the same departments relation.
	benchmarkIssue26465ConstraintRewrite(
		b,
		"users_copy/observed_349_departments_rewrites",
		fixture.departments,
		issue26465UserFKeys,
	)
	benchmarkIssue26465ConstraintRewrite(
		b,
		"users_copy/bounded_one_departments_rewrite",
		fixture.departments,
		1,
	)
}

func benchmarkIssue26465ConstraintRewrite(
	b *testing.B,
	name string,
	constraint *ConstraintDef,
	rewrites int,
) {
	b.Run(name, func(b *testing.B) {
		encodedSize := issue26465ConstraintSize(constraint)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			for range rewrites {
				for range issue26465MarshalsPerConstraintUpdate {
					encoded, err := constraint.MarshalBinary()
					if err != nil {
						b.Fatal(err)
					}
					issue26465BenchmarkSink ^= len(encoded)
				}
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(rewrites), "constraint-updates/op")
		b.ReportMetric(
			float64(rewrites*issue26465MarshalsPerConstraintUpdate*encodedSize),
			"encoded-B/op",
		)
	})
}

func newIssue26465QAConstraintFixture() issue26465QAConstraintFixture {
	departments := issue26465Constraint(
		"departments",
		issue26465DepartmentIndexes,
		nil,
		issue26465RefChildIDs(issue26465DepartmentRefChilds, 200),
	)
	issue26465PadConstraint(departments, issue26465DepartmentBytes)

	users := issue26465Constraint(
		"users",
		issue26465UserIndexes,
		issue26465ForeignKeys(issue26465UserFKeys, []uint64{100}),
		issue26465RefChildIDs(issue26465UserRefChilds, 1000),
	)
	issue26465PadConstraint(users, issue26465UserBytes)

	candidates := issue26465Constraint(
		"candidates",
		issue26465CandidateIndexes,
		issue26465ForeignKeys(issue26465CandidateFKeys, []uint64{100, 101, 102}),
		nil,
	)
	issue26465PadConstraint(candidates, issue26465CandidateBytes)

	return issue26465QAConstraintFixture{
		departments: departments,
		users:       users,
		candidates:  candidates,
	}
}

func issue26465Constraint(
	table string,
	indexCount int,
	fkeys []*plan.ForeignKeyDef,
	refChildIDs []uint64,
) *ConstraintDef {
	indexes := make([]*plan.IndexDef, 0, indexCount)
	for i := range indexCount {
		indexes = append(indexes, &plan.IndexDef{
			IdxId:          fmt.Sprintf("%s-index-id-%04d", table, i),
			IndexName:      fmt.Sprintf("%s_index_%04d", table, i),
			Parts:          []string{"id"},
			IndexTableName: fmt.Sprintf("__mo_index_%s_%04d", table, i),
			TableExist:     true,
			Visible:        true,
		})
	}

	constraints := []Constraint{
		&IndexDef{Indexes: indexes},
		&PrimaryKeyDef{Pkey: &plan.PrimaryKeyDef{
			PkeyColId:   1,
			PkeyColName: "id",
			Names:       []string{"id"},
		}},
	}
	if len(fkeys) > 0 {
		constraints = append(constraints, &ForeignKeyDef{Fkeys: fkeys})
	}
	if len(refChildIDs) > 0 {
		constraints = append(constraints, &RefChildTableDef{Tables: refChildIDs})
	}
	return &ConstraintDef{Cts: constraints}
}

func issue26465ForeignKeys(count int, parents []uint64) []*plan.ForeignKeyDef {
	fkeys := make([]*plan.ForeignKeyDef, 0, count)
	for i := range count {
		fkeys = append(fkeys, &plan.ForeignKeyDef{
			Name:        fmt.Sprintf("qa-fk-%04d", i),
			Cols:        []uint64{2},
			ForeignTbl:  parents[i%len(parents)],
			ForeignCols: []uint64{1},
			OnDelete:    plan.ForeignKeyDef_SET_NULL,
			OnUpdate:    plan.ForeignKeyDef_CASCADE,
		})
	}
	return fkeys
}

func issue26465RefChildIDs(count int, first uint64) []uint64 {
	ids := make([]uint64, count)
	for i := range ids {
		ids[i] = first + uint64(i)
	}
	return ids
}

func issue26465PadConstraint(constraint *ConstraintDef, expectedSize int) {
	indexDef := constraint.Cts[0].(*IndexDef)
	if len(indexDef.Indexes) == 0 {
		panic("QA fixture requires an index to hold deterministic padding")
	}

	padding := expectedSize - issue26465ConstraintSize(constraint)
	for range 8 {
		if padding < 0 {
			panic("QA fixture is larger than the observed constraint blob")
		}
		indexDef.Indexes[0].Comment = strings.Repeat("x", padding)
		actualSize := issue26465ConstraintSize(constraint)
		if actualSize == expectedSize {
			return
		}
		padding += expectedSize - actualSize
	}
	panic("cannot calibrate QA fixture constraint size")
}

func issue26465ConstraintSize(constraint *ConstraintDef) int {
	encoded, err := constraint.MarshalBinary()
	if err != nil {
		panic(err)
	}
	return len(encoded)
}

func assertIssue26465ConstraintShape(
	t *testing.T,
	constraint *ConstraintDef,
	wantIndexes int,
	wantFKeys int,
	wantRefChilds int,
	wantBytes int,
) {
	t.Helper()

	var indexes, fkeys, refChildren int
	for _, current := range constraint.Cts {
		switch current := current.(type) {
		case *IndexDef:
			indexes += len(current.Indexes)
		case *ForeignKeyDef:
			fkeys += len(current.Fkeys)
		case *RefChildTableDef:
			refChildren += len(current.Tables)
		}
	}
	if indexes != wantIndexes || fkeys != wantFKeys || refChildren != wantRefChilds {
		t.Fatalf(
			"unexpected QA fixture cardinality: indexes=%d fkeys=%d ref-children=%d",
			indexes,
			fkeys,
			refChildren,
		)
	}
	if actualBytes := issue26465ConstraintSize(constraint); actualBytes != wantBytes {
		t.Fatalf("unexpected QA fixture size: got %d, want %d", actualBytes, wantBytes)
	}
}
