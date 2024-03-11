// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"bytes"
	"context"
	"net"
	"testing"

	"github.com/google/uuid"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

var dummyRealHardwareAddr = func(ctx context.Context) (net.HardwareAddr, error) {
	return net.ParseMAC("3e:bf:9f:39:60:c8")
}
var dummyDockerHardwareAdder = func(ctx context.Context) (net.HardwareAddr, error) {
	return net.ParseMAC("02:42:ac:11:00:02")
}

func TestSetUUIDNodeID(t *testing.T) {
	type args struct {
		nodeUuid []byte
	}
	type fields struct {
		prepare func() *gostub.Stubs
	}
	nodeUUID := uuid.Must(uuid.Parse("abddd266-238f-11ed-ba8c-d6aee46d73fa"))
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "normal",
			fields: fields{
				prepare: func() *gostub.Stubs {
					return gostub.Stub(&getDefaultHardwareAddr, dummyRealHardwareAddr)
				},
			},
			args: args{
				nodeUuid: nodeUUID[:],
			},
			wantErr: false,
		},
		{
			name: "normal_use_nodeUUID",
			fields: fields{
				prepare: func() *gostub.Stubs {
					return gostub.Stub(&getDefaultHardwareAddr, dummyDockerHardwareAdder)
				},
			},
			args: args{
				nodeUuid: nodeUUID[:],
			},
			wantErr: false,
		},
		{
			name: "random",
			fields: fields{
				prepare: func() *gostub.Stubs {
					return gostub.Stub(&getDefaultHardwareAddr, dummyDockerHardwareAdder)
				},
			},
			args: args{
				nodeUuid: nodeUUID[:3],
			},
			wantErr: false,
		},
		{
			name:   "non-node_uuid",
			fields: fields{},
			args: args{
				nodeUuid: []byte{},
			},
			wantErr: false,
		},
		{
			name: "non-node_uuid-with_mock",
			fields: fields{
				prepare: func() *gostub.Stubs {
					return gostub.Stub(&getDefaultHardwareAddr, dummyDockerHardwareAdder)
				},
			},
			args: args{
				nodeUuid: []byte{},
			},
			wantErr: false,
		},
	}
	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stubs *gostub.Stubs
			if tt.fields.prepare != nil {
				stubs = tt.fields.prepare()
			}
			if err := SetUUIDNodeID(ctx, tt.args.nodeUuid); (err != nil) != tt.wantErr {
				t.Errorf("SetUUIDNodeID() error = %v, wantErr %v", err, tt.wantErr)
			}
			localMac, _ := getDefaultHardwareAddr(ctx)
			mockMac, _ := dummyDockerHardwareAdder(ctx)
			t.Logf("local mac: %s", localMac)
			t.Logf("mock  mac: %s", mockMac)
			t.Logf("nodeUUID : %x", tt.args.nodeUuid)
			id, _ := uuid.NewV7()
			t.Logf("uuid 1: %s", id)
			id, _ = uuid.NewV7()
			t.Logf("uuid 2: %s", id)
			require.Equal(t, false, bytes.Equal(id[10:13], dockerMacPrefix))
			if stubs != nil {
				stubs.Reset()
			}
		})
	}
}
