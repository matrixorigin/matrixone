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

package frontend

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/smartystreets/goconvey/convey"
)

func TestEventSchedulerDefaultDisabled(t *testing.T) {
	convey.Convey("event_scheduler default should be DISABLED", t, func() {
		sv, ok := gSysVarsDefs["event_scheduler"]
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(sv.Default, convey.ShouldEqual, "DISABLED")
		// DISABLED must be a valid enum value so the default is convertible.
		got, err := sv.Type.Convert(sv.Default)
		convey.So(err, convey.ShouldBeNil)
		convey.So(got, convey.ShouldEqual, "DISABLED")
	})
}

func TestLockWaitTimeoutDefaultIsBounded(t *testing.T) {
	convey.Convey("lock_wait_timeout default should fail fast", t, func() {
		sv, ok := gSysVarsDefs["lock_wait_timeout"]
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(sv.Default, convey.ShouldEqual, defaultLockWaitTimeoutSeconds)

		got, err := sv.Type.Convert(sv.Default)
		convey.So(err, convey.ShouldBeNil)
		convey.So(got, convey.ShouldEqual, defaultLockWaitTimeoutSeconds)
	})
}

func TestScope(t *testing.T) {
	convey.Convey("test scope", t, func() {
		wanted := make(map[Scope]string)
		for i := ScopeGlobal; i <= ScopeBoth; i++ {
			wanted[i] = i.String()
		}

		convey.So(wanted[ScopeBoth], convey.ShouldEqual, ScopeBoth.String())
	})
}

func TestSystemVariablesWorkloadPolicySnapshotIsCachedAndImmutable(t *testing.T) {
	raw := `{"version":1,"policies":{"ap":{"pool":"ap","labels":{"role":"ap"}}}}`
	vars := &SystemVariables{mp: map[string]interface{}{queryWorkloadPolicy: raw}}

	first := vars.WorkloadPolicySnapshot()
	require.Empty(t, first.InvalidReason)
	rule := first.Rules[schedule.WorkloadAP]
	rule.Labels["role"] = "mutated"
	first.Rules[schedule.WorkloadAP] = rule

	second := vars.WorkloadPolicySnapshot()
	require.Equal(t, "ap", second.Rules[schedule.WorkloadAP].Labels["role"])

	vars.Set(queryWorkloadPolicy, `{"version":1`)
	invalid := vars.WorkloadPolicySnapshot()
	require.NotEmpty(t, invalid.InvalidReason)
}

func TestSystemVariablesSharedWorkloadPolicySnapshotDoesNotAllocate(t *testing.T) {
	raw := `{"version":1,"policies":{"tp":{"pool":"tp","labels":{"role":"tp"},"current_cn":"required"}}}`
	vars := &SystemVariables{mp: map[string]interface{}{queryWorkloadPolicy: raw}}
	require.NotEmpty(t, vars.workloadPolicySnapshotShared().Generation)

	allocs := testing.AllocsPerRun(1000, func() {
		if vars.workloadPolicySnapshotShared().Generation == "" {
			panic("cached workload policy generation disappeared")
		}
	})
	require.Zero(t, allocs)
}

func TestGlobalSysVarsMgrWorkloadPolicyUpdateIsOrderedAndAccountScoped(t *testing.T) {
	const accountID = uint32(42)
	oldPolicy := `{"version":1,"policies":{"ap":{"pool":"old","labels":{"role":"ap"}}}}`
	newPolicy := `{"version":1,"policies":{"ap":{"pool":"new","labels":{"role":"ap"}}}}`
	vars := &SystemVariables{
		mp: map[string]interface{}{queryWorkloadPolicy: oldPolicy},
	}
	mgr := &GlobalSysVarsMgr{
		accountsGlobalSysVarsMap: map[uint32]*SystemVariables{
			accountID: vars,
		},
	}
	older := timestamp.Timestamp{PhysicalTime: 10}
	newer := timestamp.Timestamp{PhysicalTime: 20}

	require.True(t, mgr.ApplyWorkloadPolicyUpdate(accountID, newPolicy, newer))
	require.False(t, mgr.ApplyWorkloadPolicyUpdate(accountID, oldPolicy, older))
	require.False(t, mgr.ApplyWorkloadPolicyUpdate(accountID, oldPolicy, newer))
	require.Equal(t, newPolicy, vars.Get(queryWorkloadPolicy))
	require.False(t, mgr.ApplyWorkloadPolicyUpdate(accountID+1, oldPolicy, newer))
}

func TestSystemVariablesCatalogSnapshotPreservesConcurrentPolicyUpdate(t *testing.T) {
	oldPolicy := `{"version":1,"policies":{"ap":{"pool":"old","labels":{"role":"ap"}}}}`
	newPolicy := `{"version":1,"policies":{"ap":{"pool":"new","labels":{"role":"ap"}}}}`
	oldCommit := timestamp.Timestamp{PhysicalTime: 10}
	vars := &SystemVariables{
		mp:                     map[string]interface{}{queryWorkloadPolicy: oldPolicy},
		workloadPolicyCommitTS: oldCommit,
	}

	require.True(t, vars.applyWorkloadPolicyUpdate(
		newPolicy,
		timestamp.Timestamp{PhysicalTime: 20},
	))
	vars.applyCatalogSnapshot(map[string]interface{}{
		queryWorkloadPolicy: oldPolicy,
		"sql_mode":          "STRICT_TRANS_TABLES",
	}, oldCommit)

	require.Equal(t, newPolicy, vars.Get(queryWorkloadPolicy))
	require.Equal(t, "STRICT_TRANS_TABLES", vars.Get("sql_mode"))
}

func TestWorkloadPolicyRefreshWaitIsCancellable(t *testing.T) {
	vars := &SystemVariables{
		mp: make(map[string]interface{}),
		workloadPolicyRefresh: &workloadPolicyRefreshCall{
			done: make(chan struct{}),
		},
	}
	vars.workloadPolicyRefreshAfter.Store(time.Now().Add(-time.Second).UnixNano())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, vars.RefreshWorkloadPolicy(ctx, &Session{}), context.Canceled)
}

func TestSystemVariable(t *testing.T) {
	convey.Convey("all", t, func() {
		bt := SystemVariableBoolType{}
		it := SystemVariableIntType{}
		ut := SystemVariableUintType{}
		dt := SystemVariableDoubleType{}
		et := SystemVariableEnumType{}
		sett := SystemVariableSetType{}
		st := SystemVariableStringType{}
		nt := SystemVariableNullType{}
		svs := []SystemVariableType{
			bt,
			it,
			ut,
			dt,
			et,
			sett,
			st,
			nt,
		}

		for _, sv := range svs {
			_ = fmt.Sprintln(sv.String(), sv.Type(), sv.MysqlType(), sv.Zero())
		}

		btrt, err := nt.Convert(nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(btrt, convey.ShouldBeNil)

		_, err = nt.Convert("string")
		convey.So(err, convey.ShouldNotBeNil)

		_, err = bt.Convert(0)
		convey.So(err, convey.ShouldBeNil)

		_, err = bt.Convert(1)
		convey.So(err, convey.ShouldBeNil)

	})
}

func Test_valueIsBoolTrue(t *testing.T) {
	type args struct {
		value interface{}
	}
	dumpWantErr := func(t assert.TestingT, err error, msgAndArgs ...interface{}) bool {
		return false
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "bool/true",
			args: args{
				value: true,
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "bool/false",
			args: args{
				value: false,
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "int/1",
			args: args{
				value: 1,
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "int/0",
			args: args{
				value: 0,
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "int8/1",
			args: args{
				value: int8(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "int8/0",
			args: args{
				value: int8(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "int16/1",
			args: args{
				value: int16(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "int16/0",
			args: args{
				value: int16(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "int32/1",
			args: args{
				value: int32(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "int32/0",
			args: args{
				value: int32(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "int64/1",
			args: args{
				value: int64(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "int64/0",
			args: args{
				value: int64(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "uint8/1",
			args: args{
				value: uint8(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "uint8/0",
			args: args{
				value: uint8(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "uint16/1",
			args: args{
				value: uint16(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "uint16/0",
			args: args{
				value: uint16(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "uint32/1",
			args: args{
				value: uint32(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "uint32/0",
			args: args{
				value: uint32(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "uint64/1",
			args: args{
				value: uint64(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "uint64/0",
			args: args{
				value: uint64(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "string/on",
			args: args{
				value: "ON",
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "string/off",
			args: args{
				value: "OFF",
			},
			want:    false,
			wantErr: dumpWantErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := valueIsBoolTrue(tt.args.value)
			if !tt.wantErr(t, err, fmt.Sprintf("valueIsBoolTrue(%v)", tt.args.value)) {
				return
			}
			assert.Equalf(t, tt.want, got, "valueIsBoolTrue(%v)", tt.args.value)
		})
	}
}
