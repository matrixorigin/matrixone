// Copyright 2025 Matrix Origin
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

package frontend

import "testing"

func TestCanExecPublicationInternalCmd(t *testing.T) {
	tests := []struct {
		name   string
		tenant *TenantInfo
		want   bool
	}{
		{name: "nil tenant", want: false},
		{
			name: "normal tenant",
			tenant: &TenantInfo{
				Tenant:   "acc1",
				TenantID: 1001,
				User:     "acc1:user:accountadmin",
			},
			want: false,
		},
		{
			name: "sys tenant",
			tenant: &TenantInfo{
				Tenant:   sysAccountName,
				TenantID: sysAccountID,
				User:     "sys:user:public",
			},
			want: true,
		},
		{
			name: "sys moadmin",
			tenant: &TenantInfo{
				Tenant:   sysAccountName,
				TenantID: sysAccountID,
				User:     DefaultTenantMoAdmin,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := canExecPublicationInternalCmd(tt.tenant); got != tt.want {
				t.Fatalf("canExecPublicationInternalCmd() = %v, want %v", got, tt.want)
			}
		})
	}
}
