package motrace

import (
	"errors"
	"testing"
	"time"
)

func TestAggregator(t *testing.T) {
	var sessionId [16]byte
	sessionId[0] = 1

	aggregator := NewAggregator(
		7*time.Second,
		func() Item {
			return &StatementInfo{}
		},
		func(existing, new Item) {
			e := existing.(*StatementInfo)
			n := new.(*StatementInfo)
			e.Duration += n.Duration
		},
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

	// Get results from aggregator
	results := aggregator.GetResults()

	// Test expected behavior
	if len(results) != 0 {
		t.Errorf("Expected 0 aggregated statements, got %d", len(results))
	}
}
