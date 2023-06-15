package motrace

import (
	"errors"
	"testing"
	"time"
)

var ErrFilteredOut = errors.New("item filtered out")

func TestAggregator(t *testing.T) {
	var sessionId [16]byte
	sessionId[0] = 1

	// Local version of statementInfoFilterFunc for testing
	statementInfoFilterFunc := func(i Item) bool {
		s, ok := i.(*StatementInfo)
		if !ok {
			return false
		}
		if s.StatementType != "Select" && s.StatementType != "Insert" && s.StatementType != "Update" && s.StatementType != "Execute" {
			return false
		}
		if s.Duration < 200*time.Millisecond && s.StatementType == "Select" {
			return false
		}
		if s.SqlSourceType != "Internal" && s.SqlSourceType != "External" && s.SqlSourceType != "Non_Cloud_User" {
			return false
		}
		return true
	}

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
		statementInfoFilterFunc,
	)

	// Insert StatementInfo instances into the aggregator
	_, err := aggregator.AddItem(&StatementInfo{
		StatementType: "Type1",
		Duration:      time.Duration(5 * time.Second),
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
