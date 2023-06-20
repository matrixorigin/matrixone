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
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestAggregator(t *testing.T) {

	var sessionId [16]byte
	sessionId[0] = 1
	var sessionId2 [16]byte
	sessionId2[0] = 2
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
	fixedTime := time.Date(2023, time.June, 10, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		_, err = aggregator.AddItem(&StatementInfo{
			Account:       "MO",
			User:          "moroot",
			Database:      "system",
			StatementType: "Select",
			SqlSourceType: "external_sql",
			SessionID:     sessionId,
			Statement:     "SELECT 11", // make it longer than 200ms to pass filter
			RequestAt:     fixedTime,
			Duration:      10 * time.Millisecond,
			TransactionID: _1TxnID,
			StatementID:   _1TxnID,
			Status:        StatementStatusSuccess,
			ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
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
			Statement:     "SELECT 11", // make it longer than 200ms to pass filter
			RequestAt:     fixedTime,
			Duration:      10 * time.Millisecond,
			TransactionID: _1TxnID,
			StatementID:   _1TxnID,
			Status:        StatementStatusSuccess,
			ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
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
			Statement:     "SELECT 11", // make it longer than 200ms to pass filter
			RequestAt:     fixedTime.Add(6 * time.Second),
			Duration:      10 * time.Millisecond,
			TransactionID: _1TxnID,
			StatementID:   _1TxnID,
			Status:        StatementStatusSuccess,
			ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
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
			RequestAt:     fixedTime.Add(6 * time.Second),
			Duration:      10 * time.Millisecond,
			TransactionID: _1TxnID,
			StatementID:   _1TxnID,
			Status:        StatementStatusFailed,
			ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
		})
		if err != nil {
			t.Fatalf("Unexpected error when adding item: %v", err)
		}
	}

	// Get results from aggregator
	results = aggregator.GetResults()

	// Test expected behavior
	if len(results) != 4 {
		t.Errorf("Expected 0 aggregated statements, got %d", len(results))
	}
	assert.Equal(t, 50*time.Millisecond, results[0].(*StatementInfo).Duration)
	assert.Equal(t, 50*time.Millisecond, results[1].(*StatementInfo).Duration)
	assert.Equal(t, 50*time.Millisecond, results[2].(*StatementInfo).Duration)
	assert.Equal(t, 50*time.Millisecond, results[3].(*StatementInfo).Duration)

	aggregator.Close()
	// Update
	for i := 0; i < 5; i++ {

		_, err = aggregator.AddItem(&StatementInfo{
			Account:       "MO",
			User:          "moroot",
			Database:      "system",
			StatementType: "Update",
			SqlSourceType: "external_sql",
			SessionID:     sessionId2,
			Statement:     "Update 11", // make it longer than 200ms to pass filter
			RequestAt:     fixedTime.Add(6 * time.Second),
			Duration:      10 * time.Millisecond,
			TransactionID: _1TxnID,
			StatementID:   _1TxnID,
			Status:        StatementStatusFailed,
			ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
		})
		if err != nil {
			t.Fatalf("Unexpected error when adding item: %v", err)
		}
	}
	results = aggregator.GetResults()

	assert.Equal(t, "Update 11; Update 11; Update 11; Update 11; Update 11", results[0].(*StatementInfo).StmtBuilder.String())

}
