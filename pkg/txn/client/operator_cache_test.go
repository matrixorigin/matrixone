package client

import (
	"testing"
)

func TestOperatorCache(t *testing.T) {
	tc := &txnOperator{}

	tc.Set("key1", "value1")
	val, ok := tc.Get("key1")
	if !ok {
		t.Fatalf("expected key1 to be present")
	}
	if val.(string) != "value1" {
		t.Fatalf("expected value1, got %v", val)
	}

	tc.Delete("key1")
	_, ok = tc.Get("key1")
	if ok {
		t.Fatalf("expected key1 to be deleted")
	}
}
