// Copyright 2022 Matrix Origin
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

package trace

import "encoding/json"

const (
	MOStatementType = "MOStatementType"
	MOSpanType      = "MOSpan"
	MOLogType       = "MOLog"
	MOZapType       = "MOZap"
	MOErrorType     = "MOError"
)

const (
	B int64 = 1 << (iota * 10)
	KB
	MB
	GB
)

type Resource struct {
	m map[string]any
}

func newResource() *Resource {
	return &Resource{m: make(map[string]any)}

}

func (r *Resource) Put(key string, val any) {
	r.m[key] = val
}

func (r *Resource) Get(key string) (any, bool) {
	val, has := r.m[key]
	return val, has
}

// String need to improve
func (r *Resource) String() string {
	buf, _ := json.Marshal(r.m)
	return string(buf)

}

type NodeType int

const (
	NodeTypeNode NodeType = iota
	NodeTypeCN
	NodeTypeDN
	NodeTypeLogService
)

func (t NodeType) String() string {
	switch t {
	case NodeTypeNode:
		return "Node"
	case NodeTypeCN:
		return "CN"
	case NodeTypeDN:
		return "DN"
	case NodeTypeLogService:
		return "LogService"
	default:
		return "Unknown"
	}
}

type MONodeResource struct {
	NodeID   int64    `json:"node_id"`
	NodeType NodeType `json:"node_type"`
}
