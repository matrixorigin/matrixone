package rpc

import (
	"bytes"
	"testing"
)

func TestXxx(t *testing.T) {
	b := &bytes.Buffer{}
	RunInspect("catalog -v", b)
	t.Log("\n", b.String())
}
