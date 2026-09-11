// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	"github.com/stretchr/testify/require"
)

func TestTaskDescriptorIncludesLosslessCDCCode(t *testing.T) {
	zr, err := gzip.NewReader(bytes.NewReader(proto.FileDescriptor("task.proto")))
	require.NoError(t, err)
	raw, err := io.ReadAll(zr)
	require.NoError(t, err)
	require.NoError(t, zr.Close())
	var fd descriptor.FileDescriptorProto
	require.NoError(t, proto.Unmarshal(raw, &fd))
	for _, enum := range fd.EnumType {
		if enum.GetName() != "TaskCode" {
			continue
		}
		for _, value := range enum.Value {
			if value.GetName() == "InitCdcLosslessStart" {
				require.Equal(t, int32(15), value.GetNumber())
				return
			}
		}
	}
	t.Fatal("task descriptor is missing InitCdcLosslessStart=15")
}

func TestDetailsType(t *testing.T) {
	tests := []struct {
		name    string
		details *Details
		want    TaskType
	}{
		{"nil", nil, TaskType_TypeUnknown},
		{"empty", &Details{}, TaskType_TypeUnknown},
		{"cdc", &Details{Details: &Details_CreateCdc{}}, TaskType_CreateCdc},
		{"iscp", &Details{Details: &Details_ISCP{}}, TaskType_ISCP},
		{"publication", &Details{Details: &Details_Publication{}}, TaskType_Publication},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, test.details.Type())
		})
	}
}

func TestDetailsPreservesRemovedOneofWire(t *testing.T) {
	// Field 10 was the Connector oneof. New binaries must retain it while
	// retiring the daemon task, even though it no longer has a generated type.
	legacy := &Details{}
	require.NoError(t, legacy.Unmarshal(
		[]byte{0x52, 0x06, 0x0a, 0x04, 'd', 'b', '.', 't'},
	))
	require.Equal(t, TaskType_TypeUnknown, legacy.Type())
	require.NotEmpty(t, legacy.XXX_unrecognized)

	wire, err := legacy.Marshal()
	require.NoError(t, err)
	roundTrip := &Details{}
	require.NoError(t, roundTrip.Unmarshal(wire))
	require.Equal(t, legacy.XXX_unrecognized, roundTrip.XXX_unrecognized)
}
