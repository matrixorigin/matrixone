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
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
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
			if field.GetName() == "asof_right_col" && field.GetNumber() == 86 {
				return
			}
		}
	}
	t.Fatal("plan descriptor missing Node.asof_right_col = 86")
}

func TestGeneratedPlanDescriptorContainsAsofContract(t *testing.T) {
	b, _ := (&Node{}).Descriptor()
	file := decodePlanDescriptor(t, b)
	var node *descriptor.DescriptorProto
	for _, message := range file.GetMessageType() {
		if message.GetName() == "Node" {
			node = message
			break
		}
	}
	if node == nil {
		t.Fatal("generated descriptor missing Node")
	}
	for _, field := range node.GetField() {
		if field.GetName() == "asof_right_col" && field.GetNumber() == 86 {
			return
		}
	}
	t.Fatal("generated Node descriptor missing asof_right_col = 86")
}

func TestGeneratedJoinTypeEnumDescriptorContainsAsofValues(t *testing.T) {
	b, path := Node_ASOF.EnumDescriptor()
	file := decodePlanDescriptor(t, b)
	if len(path) != 2 {
		t.Fatalf("unexpected Node.JoinType descriptor path: %v", path)
	}
	node := file.GetMessageType()[path[0]]
	joinType := node.GetEnumType()[path[1]]
	want := map[string]int32{"ASOF": 11, "ASOF_LEFT": 12}
	for _, value := range joinType.GetValue() {
		if number, ok := want[value.GetName()]; ok && number == value.GetNumber() {
			delete(want, value.GetName())
		}
	}
	if len(want) != 0 {
		t.Fatalf("generated Node.JoinType descriptor missing ASOF values: %v", want)
	}
}

func TestCreateTableDefaultsAndForeignKeyWireContract(t *testing.T) {
	b, path := (&CreateTable{}).Descriptor()
	message := decodePlanDescriptor(t, b).GetMessageType()[path[0]]
	wantFields := map[string]int32{
		"RawSQL":                15,
		"unresolved_fk_parents": 16,
		"fk_name_binding":       17,
		"database_defaults":     18,
	}
	for _, field := range message.GetField() {
		if number, ok := wantFields[field.GetName()]; ok {
			if field.GetNumber() != number {
				t.Fatalf("%s field number: got %d, want %d", field.GetName(), field.GetNumber(), number)
			}
			delete(wantFields, field.GetName())
		}
	}
	if len(wantFields) != 0 {
		t.Fatalf("missing CreateTable fields: %v", wantFields)
	}
	reserved := false
	for _, span := range message.GetReservedRange() {
		reserved = reserved || span.GetStart() <= 14 && span.GetEnd() > 14
	}
	if !reserved {
		t.Fatal("CreateTable field 14 must remain reserved")
	}

	defaults := &DatabaseDefaults{
		DatabaseId: 42, CharacterSet: "utf8mb4", Collation: "utf8mb4_bin",
		Version: 7, CollationId: 2, CollationRevision: 0,
	}
	parents := []*ObjectRef{{SchemaName: "parent_db", ObjName: "parent_table", Obj: 91}}
	binding := &lock.LockTable{Table: 93, Version: 4, ServiceID: "cn", Valid: true}
	for _, tc := range []struct {
		name string
		plan *CreateTable
	}{
		{"empty", &CreateTable{}},
		{"defaults_only", &CreateTable{DatabaseDefaults: defaults}},
		{"foreign_keys_only", &CreateTable{UnresolvedFkParents: parents, FkNameBinding: binding}},
		{"combined", &CreateTable{RawSQL: "create table child(id int)", DatabaseDefaults: defaults,
			UnresolvedFkParents: parents, FkNameBinding: binding}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := tc.plan.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			if len(encoded) != tc.plan.ProtoSize() {
				t.Fatal("CreateTable wire size mismatch")
			}
			var decoded CreateTable
			if err := decoded.Unmarshal(encoded); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(tc.plan, &decoded) {
				t.Fatalf("CreateTable wire round trip lost defaults or foreign-key admission: %v", &decoded)
			}
			cloned := proto.Clone(tc.plan).(*CreateTable)
			if !proto.Equal(tc.plan, cloned) {
				t.Fatal("CreateTable clone lost metadata")
			}
			if cloned.DatabaseDefaults != nil {
				cloned.DatabaseDefaults.Version++
				if tc.plan.DatabaseDefaults.Version != 7 {
					t.Fatal("CreateTable clone aliases database defaults")
				}
			}
		})
	}
}

func decodePlanDescriptor(t *testing.T, b []byte) *descriptor.FileDescriptorProto {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	var file descriptor.FileDescriptorProto
	if err := proto.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	return &file
}
