package plugin

import "testing"

func TestRequest(t *testing.T) {
	m := &Request{}
	m.SetID(1)
	if m.GetID() != 1 {
		t.Errorf("Request.GetID() = %v, want %v", m.GetID(), 1)
	}
	if m.DebugString() != "1: <nil>/" {
		t.Errorf("Request.DebugString() = %v, want %v", m.DebugString(), "1: <nil>/")
	}
}

func TestResponse(t *testing.T) {
	m := &Response{}
	m.SetID(1)
	if m.GetID() != 1 {
		t.Errorf("Response.GetID() = %v, want %v", m.GetID(), 1)
	}
	if m.DebugString() != "1: <nil>/" {
		t.Errorf("Response.DebugString() = %v, want %v", m.DebugString(), "1: <nil>/")
	}
}
