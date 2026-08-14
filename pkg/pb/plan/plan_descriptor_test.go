// Copyright 2026 Matrix Origin
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

package plan

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
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

func TestNodeAsofRightColRoundTrip(t *testing.T) {
	original := &Node{AsofRightCol: 7, PartitionByCount: 3}
	b, err := original.Marshal()
	if err != nil {
		t.Fatalf("marshal node: %v", err)
	}
	var decoded Node
	if err = decoded.Unmarshal(b); err != nil {
		t.Fatalf("unmarshal node: %v", err)
	}
	if decoded.AsofRightCol != original.AsofRightCol || decoded.PartitionByCount != original.PartitionByCount {
		t.Fatalf("node round-trip mismatch: got asof=%d partition=%d", decoded.AsofRightCol, decoded.PartitionByCount)
	}
}

func TestPlanDescriptorContainsAsofFields(t *testing.T) {
	b := proto.FileDescriptor("plan.proto")
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	var file descriptor.FileDescriptorProto
	if err = proto.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	for _, message := range file.GetMessageType() {
		if message.GetName() != "Node" {
			continue
		}
		for _, field := range message.GetField() {
			if field.GetName() == "asof_right_col" && field.GetNumber() == 81 {
				return
			}
		}
	}
	t.Fatal("plan descriptor missing Node.asof_right_col = 81")
}
