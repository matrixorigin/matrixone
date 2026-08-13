package plan

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/gogo/protobuf/proto"
)

func TestPlanFileDescriptorIsGzipEncoded(t *testing.T) {
	b := proto.FileDescriptor("plan.proto")
	if len(b) < 2 {
		t.Fatalf("plan descriptor is empty")
	}
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("plan descriptor is not valid gzip: %v", err)
	}
	defer r.Close()
	buf := make([]byte, 1)
	if _, err = r.Read(buf); err != nil {
		t.Fatalf("plan descriptor payload cannot be decoded: %v", err)
	}
}
