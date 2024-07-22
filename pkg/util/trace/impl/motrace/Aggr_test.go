// Copyright 2023 Matrix Origin
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

package motrace

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

func TestAggregator(t *testing.T) {

	var sessionId = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x1}
	var sessionId2 = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x11}
	const aggrWindow = 5 * time.Second

	ctx := context.Background()
	aggregator := NewAggregator(
		ctx,
		aggrWindow,
		StatementInfoNew,
		StatementInfoUpdate,
		StatementInfoFilter,
	)

	// Insert StatementInfo instances into the aggregator
	_, err := aggregator.AddItem(&StatementInfo{
		StatementType: "Grant",
		Duration:      time.Duration(500 * time.Millisecond), // make it longer than 200ms to pass filter
		SqlSourceType: "internal_sql",
	})

	if !errors.Is(err, ErrFilteredOut) {
		t.Fatalf("Expected error ErrFilteredOut, got: %v", err)
	}

	// Insert StatementInfo instances into the aggregator
	_, err = aggregator.AddItem(&StatementInfo{
		StatementType: "Select",
		Duration:      time.Duration(500 * time.Millisecond), // make it longer than 200ms to pass filter
		SqlSourceType: "internal_sql",
	})

	if !errors.Is(err, ErrFilteredOut) {
		t.Fatalf("Expected error ErrFilteredOut, got: %v", err)
	}

	_, err = aggregator.AddItem(&StatementInfo{
		StatementType: "Type1",
		Duration:      time.Duration(10 * time.Second),
	})

	if !errors.Is(err, ErrFilteredOut) {
		t.Fatalf("Expected error ErrFilteredOut, got: %v", err)
	}

	_, err = aggregator.AddItem(&StatementInfo{
		StatementType: "Insert",
		SqlSourceType: "cloud_user",
		Duration:      time.Duration(10 * time.Second),
	})

	if !errors.Is(err, ErrFilteredOut) {
		t.Fatalf("Expected error ErrFilteredOut, got: %v", err)
	}

	// Get results from aggregator
	results := aggregator.GetResults()

	// Test expected behavior
	if len(results) != 0 {
		t.Errorf("Expected 0 aggregated statements, got %d", len(results))
	}

	// Aggregate some Select
	fixedTime := time.Date(2023, time.June, 10, 12, 0, 1, 0, time.UTC)
	for i := 0; i < 5; i++ {
		_, err = aggregator.AddItem(&StatementInfo{
			Account:       "MO",
			User:          "moroot",
			Database:      "system",
			StatementType: "Select",
			SqlSourceType: "external_sql",
			SessionID:     sessionId,
			Statement:     "SELECT 11",
			ResponseAt:    fixedTime,
			RequestAt:     fixedTime.Add(-10 * time.Millisecond),
			Duration:      10 * time.Millisecond,
			TransactionID: _1TxnID,
			StatementID:   _1StmtID,
			Status:        StatementStatusSuccess,
			ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
			ConnType:      statistic.ConnTypeExternal,
		})
		if err != nil {
			t.Fatalf("Unexpected error when adding item: %v", err)
		}

		// different session id
		_, err = aggregator.AddItem(&StatementInfo{
			Account:       "MO",
			User:          "moroot",
			Database:      "system",
			StatementType: "Select",
			SqlSourceType: "internal_sql",
			SessionID:     sessionId2,
			Statement:     "SELECT 11",
			ResponseAt:    fixedTime,
			RequestAt:     fixedTime.Add(-10 * time.Millisecond),
			Duration:      10 * time.Millisecond,
			TransactionID: _1TxnID,
			StatementID:   _1StmtID,
			Status:        StatementStatusSuccess,
			ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
			ConnType:      statistic.ConnTypeExternal,
		})
		if err != nil {
			t.Fatalf("Unexpected error when adding item: %v", err)
		}

		// same as the second session id with 5 seconds later
		_, err = aggregator.AddItem(&StatementInfo{
			Account:       "MO",
			User:          "moroot",
			Database:      "system",
			StatementType: "Select",
			SqlSourceType: "internal_sql",
			SessionID:     sessionId2,
			Statement:     "SELECT 11",
			ResponseAt:    fixedTime.Add(6 * time.Second),
			RequestAt:     fixedTime.Add(6 * time.Second).Add(-10 * time.Millisecond),
			Duration:      10 * time.Millisecond,
			TransactionID: _1TxnID,
			StatementID:   _1StmtID,
			Status:        StatementStatusSuccess,
			ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
			ConnType:      statistic.ConnTypeExternal,
		})

		if err != nil {
			t.Fatalf("Unexpected error when adding item: %v", err)
		}

		// Error status
		_, err = aggregator.AddItem(&StatementInfo{
			Account:       "MO",
			User:          "moroot",
			Database:      "system",
			StatementType: "Select",
			SqlSourceType: "external_sql",
			SessionID:     sessionId2,
			Statement:     "SELECT 11", // make it longer than 200ms to pass filter
			ResponseAt:    fixedTime.Add(6 * time.Second),
			RequestAt:     fixedTime.Add(6 * time.Second).Add(-10 * time.Millisecond),
			Duration:      10 * time.Millisecond,
			TransactionID: _1TxnID,
			StatementID:   _1StmtID,
			Status:        StatementStatusFailed,
			ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
			ConnType:      statistic.ConnTypeExternal,
		})
		if err != nil {
			t.Fatalf("Unexpected error when adding item: %v", err)
		}
	}

	// Get results from aggregator
	results = aggregator.GetResults()

	// Test expected behavior
	if len(results) != 4 {
		t.Errorf("Expected 4 aggregated statements, got %d", len(results))
	}
	assert.Equal(t, 50*time.Millisecond, results[0].(*StatementInfo).Duration)
	assert.Equal(t, 50*time.Millisecond, results[1].(*StatementInfo).Duration)
	assert.Equal(t, 50*time.Millisecond, results[2].(*StatementInfo).Duration)
	assert.Equal(t, 50*time.Millisecond, results[3].(*StatementInfo).Duration)
	for idx := 0; idx < 4; idx++ {
	}
	targetBytes := []byte(`[4,5,10.000,15,20,25,2,0,220.0803]`)
	for idx := 0; idx < 4; idx++ {
		require.Equal(t, targetBytes, results[idx].(*StatementInfo).GetStatsArrayBytes())
	}
	item, _ := results[0].(*StatementInfo)
	row := item.GetTable().GetRow(ctx)
	targetBytes = []byte(`[4,5,2.000,15,20,25,2,0,220.0803]`) // re-calculate memory usage in FillRow
	for idx := 0; idx < 4; idx++ {
		results[idx].(*StatementInfo).FillRow(ctx, row)
		require.Equal(t, targetBytes, results[idx].(*StatementInfo).GetStatsArrayBytes())
	}

	aggregator.Close()

	aggregator = NewAggregator(
		ctx,
		aggrWindow,
		StatementInfoNew,
		StatementInfoUpdate,
		StatementInfoFilter,
	)

	// Update
	for i := 0; i < 5; i++ {

		_, err = aggregator.AddItem(&StatementInfo{
			Account:       "MO",
			User:          "moroot",
			Database:      "system",
			StatementType: "Update",
			SqlSourceType: "external_sql",
			SessionID:     sessionId2,
			Statement:     "Update 11",
			ResponseAt:    fixedTime.Add(6 * time.Second),
			RequestAt:     fixedTime.Add(6 * time.Second).Add(-10 * time.Millisecond),
			Duration:      time.Duration(10+i) * time.Millisecond,
			TransactionID: _1TxnID,
			StatementID:   _1StmtID,
			Status:        StatementStatusFailed,
			ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
		})
		if err != nil {
			t.Fatalf("Unexpected error when adding item: %v", err)
		}

		_, err = aggregator.AddItem(&StatementInfo{
			Account:       "MO",
			User:          "moroot",
			Database:      "system",
			StatementType: "Update",
			SqlSourceType: "internal_sql",
			SessionID:     sessionId2,
			Statement:     "Update 11",
			ResponseAt:    fixedTime.Add(6 * time.Second),
			RequestAt:     fixedTime.Add(6 * time.Second).Add(-10 * time.Millisecond),
			Duration:      time.Duration(10+i) * time.Millisecond,
			TransactionID: _1TxnID,
			StatementID:   _1StmtID,
			Status:        StatementStatusFailed,
			ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
		})
		if err != nil {
			t.Fatalf("Unexpected error when adding item: %v", err)
		}
	}
	results = aggregator.GetResults()

	assert.Equal(t, "Update 11", results[0].(*StatementInfo).StmtBuilder.String())
	// should have two results since they have different sqlSourceType
	assert.Equal(t, "Update 11", results[1].(*StatementInfo).StmtBuilder.String())
	assert.Equal(t, 60*time.Millisecond, results[1].(*StatementInfo).Duration)
	// RequestAt should be starting of the window
	assert.Equal(t, fixedTime.Add(4*time.Second), results[0].(*StatementInfo).RequestAt)
	// ResponseAt should be end of the window
	assert.Equal(t, fixedTime.Add(9*time.Second), results[0].(*StatementInfo).ResponseAt)
	require.Equal(t, []byte(`[4,5,10.000,15,20,25,0,0,220.0803]`), results[0].(*StatementInfo).GetStatsArrayBytes())
	results[0].(*StatementInfo).FillRow(ctx, row) // re-calculate memory usage in FillRow
	require.Equal(t, []byte(`[4,5,2.000,15,20,25,0,0,220.0803]`), results[0].(*StatementInfo).GetStatsArrayBytes())

	_, err = aggregator.AddItem(&StatementInfo{
		Account:       "MO",
		User:          "moroot",
		Database:      "system",
		StatementType: "Update",
		SqlSourceType: "external_sql",
		SessionID:     sessionId2,
		Statement:     "Update 11",
		ResponseAt:    fixedTime.Add(6 * time.Second),
		RequestAt:     fixedTime.Add(6 * time.Second).Add(-10 * time.Millisecond),
		Duration:      203 * time.Millisecond,
		TransactionID: _1TxnID,
		StatementID:   _1StmtID,
		Status:        StatementStatusFailed,
		ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
	})
	if err != ErrFilteredOut {
		t.Fatalf("Expecting filter out error due to Duration longer than 200ms: %v", err)
	}

}

func TestAggregatorWithStmtMerge(t *testing.T) {
	c := GetTracerProvider()
	c.enableStmtMerge = true

	var sessionId = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x1}
	const aggrWindow = 5 * time.Second

	ctx := context.Background()
	aggregator := NewAggregator(
		ctx,
		aggrWindow,
		StatementInfoNew,
		StatementInfoUpdate,
		StatementInfoFilter,
	)
	var err error

	fixedTime := time.Date(2023, time.June, 10, 12, 0, 1, 0, time.UTC)
	for i := 0; i < 2; i++ {
		_, err = aggregator.AddItem(&StatementInfo{
			Account:       "MO",
			User:          "moroot",
			Database:      "system",
			StatementType: "Select",
			SqlSourceType: "external_sql",
			SessionID:     sessionId,
			Statement:     "SELECT 11",
			ResponseAt:    fixedTime,
			RequestAt:     fixedTime.Add(-10 * time.Millisecond),
			Duration:      10 * time.Millisecond,
			RowsRead:      1,
			TransactionID: _1TxnID,
			StatementID:   _1StmtID,
			Status:        StatementStatusSuccess,
			ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
		})
		if err != nil {
			t.Fatalf("Unexpected error when adding item: %v", err)
		}
	}

	// Get results from aggregator
	results := aggregator.GetResults()

	// Test expected behavior
	if len(results) != 1 {
		t.Errorf("Expected 0 aggregated statements, got %d", len(results))
	}

	assert.Equal(t, "SELECT 11;\nSELECT 11", results[0].(*StatementInfo).StmtBuilder.String())

	res := "/* " + strconv.FormatInt(results[0].(*StatementInfo).AggrCount, 10) + " queries */ \n" + results[0].(*StatementInfo).StmtBuilder.String()

	assert.Equal(t, "/* 2 queries */ \nSELECT 11;\nSELECT 11", res)

	assert.Equal(t, int64(2), results[0].(*StatementInfo).RowsRead)

}

func TestAggregator_MarkExported(t *testing.T) {
	type fields struct {
		elems int
	}
	tests := []struct {
		name   string
		fields fields
		//want   []Item
	}{
		{
			name:   "normal",
			fields: fields{elems: 5},
		},
		{
			name:   "normal_100",
			fields: fields{elems: 100},
		},
	}

	const aggrWindow = 5 * time.Second

	var err error
	var sessionId = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x1}
	var ctx = context.TODO()
	// Aggregate some Select
	var fixedTime = time.Date(2023, time.June, 10, 12, 0, 1, 0, time.UTC)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			aggregator := NewAggregator(
				ctx,
				aggrWindow,
				StatementInfoNew,
				StatementInfoUpdate,
				StatementInfoFilter,
			)

			var stmts []*StatementInfo
			for i := 0; i < tt.fields.elems; i++ {
				stmt := &StatementInfo{
					Account:       "MO",
					User:          "moroot",
					Database:      "system",
					StatementType: "Select",
					SqlSourceType: "external_sql",
					SessionID:     sessionId,
					Statement:     "SELECT 11",
					ResponseAt:    fixedTime,
					RequestAt:     fixedTime.Add(-10 * time.Millisecond),
					Duration:      10 * time.Millisecond,
					TransactionID: _1TxnID,
					StatementID:   _1StmtID,
					Status:        StatementStatusSuccess,
					ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
				}
				stmts = append(stmts, stmt)
				_, err = aggregator.AddItem(stmt)
				if err != nil {
					t.Fatalf("Unexpected error when adding item: %v", err)
				}
			}

			// Get results from aggregator
			// Check all records' exported value.
			results := aggregator.GetResults()
			require.Equal(t, 1, len(results))
			require.Equal(t, false, results[0].(*StatementInfo).exported)
			for i := 1; i < tt.fields.elems; i++ {
				require.Equal(t, true, stmts[i].exported)
			}
		})
	}
}
