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

package tree

import (
	"encoding/json"
	"strings"
)

// CentroidxOption is the option of a CENTROIDX join: its metric and, when the join assigns the
// rows of an IVF-FLAT index build or sync, the index's algo params and table config, both JSON.
type CentroidxOption struct {
	OpType string `json:"op_type"`
	Params string `json:"params,omitempty"`
	Config string `json:"config,omitempty"`
}

// String encodes o: its metric alone when it has no index, JSON otherwise.
func (o CentroidxOption) String() string {
	if o.Params == "" && o.Config == "" {
		return o.OpType
	}
	b, _ := json.Marshal(o)
	return string(b)
}

// ParseCentroidxOption decodes the option s of a CENTROIDX join.
func ParseCentroidxOption(s string) (CentroidxOption, error) {
	if !strings.HasPrefix(s, "{") {
		return CentroidxOption{OpType: s}, nil
	}
	var o CentroidxOption
	err := json.Unmarshal([]byte(s), &o)
	return o, err
}

func (o CentroidxOption) format(ctx *FmtCtx) {
	ctx.WriteString(" (")
	writeCentroidxArg(ctx, strings.ToLower(o.OpType))
	if o.Params != "" || o.Config != "" {
		ctx.WriteString(", ")
		writeCentroidxArg(ctx, o.Params)
		ctx.WriteString(", ")
		writeCentroidxArg(ctx, o.Config)
	}
	ctx.WriteString(")")
}

func writeCentroidxArg(ctx *FmtCtx, s string) {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	ctx.WriteString("'" + s + "'")
}
