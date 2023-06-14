package etl

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

func TestAggregator(t *testing.T) {
	var sessionId [16]byte
	sessionId[0] = 1

	aggregator := NewAggregator(
		7*time.Second,
		func() Item {
			return &motrace.StatementInfo{
				RequestAt: time.Now(),
			}
		},
		func(existing, new Item) {
			e := existing.(*motrace.StatementInfo)
			n := new.(*motrace.StatementInfo)
			e.Duration += n.Duration
			e.Statement += n.Statement + " "
		},
	)

	// Insert StatementInfo instances into the aggregator
	aggregator.AddItem(&motrace.StatementInfo{
		SessionID:     sessionId,
		StatementType: "Type1",
		Status:        motrace.StatementStatusSuccess,
		RequestAt:     time.Now(),
		Duration:      time.Duration(5 * time.Second),
		Statement:     "Statement1",
	})

	aggregator.AddItem(&motrace.StatementInfo{
		SessionID:     sessionId,
		StatementType: "Type1",
		Status:        motrace.StatementStatusSuccess,
		RequestAt:     time.Now(),
		Duration:      time.Duration(10 * time.Second),
		Statement:     "Statement2",
	})

	// Get results from aggregator
	results := aggregator.GetResults()

	// Test expected behavior
	if len(results) != 1 {
		t.Errorf("Expected 1 aggregated statements, got %d", len(results))
	}

	aggregatedStatement := results[0].(*motrace.StatementInfo)
	if aggregatedStatement.Duration != 15*time.Second {
		t.Errorf("Unexpected duration in aggregated statement: %d", aggregatedStatement.Duration)
	}
	if aggregatedStatement.Statement != "Statement1 Statement2 " {
		t.Errorf("Unexpected statement in aggregated statement: %s", aggregatedStatement.Statement)
	}
}
