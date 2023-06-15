package motrace

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAggregator(t *testing.T) {
	var sessionId [16]byte
	sessionId[0] = 1
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
		StatementType: "Select",
		Duration:      time.Duration(500 * time.Millisecond), // make it longer than 200ms to pass filter
		SqlSourceType: "Internal",
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
		SqlSourceType: "Cloud_User",
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

	_, err = aggregator.AddItem(&StatementInfo{
		Account:       "MO",
		User:          "moroot",
		Database:      "system",
		StatementType: "Select",
		SqlSourceType: "External",
		SessionID:     sessionId,
		Statement:     "SELECT 11", // make it longer than 200ms to pass filter
		RequestAt:     time.Now(),
		Duration:      10 * time.Millisecond,
		TransactionID: _1TxnID,
		StatementID:   _1TxnID,
		RowsRead:      1,
		BytesScan:     1,
		ResultCount:   2,
		Status:        StatementStatusSuccess,
		ExecPlan:      NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
	})

}
