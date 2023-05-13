// Copyright 2021 - 2023 Matrix Origin
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
