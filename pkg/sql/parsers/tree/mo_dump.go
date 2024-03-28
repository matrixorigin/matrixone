// Copyright 2021 Matrix Origin
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

import "github.com/matrixorigin/matrixone/pkg/common/reuse"

func init() {
	reuse.CreatePool[MoDump](
		func() *MoDump { return &MoDump{} },
		func(m *MoDump) { m.reset() },
		reuse.DefaultOptions[MoDump](),
	)

}

type MoDump struct {
	statementImpl
	ExportParams *ExportParam
}

func NewMoDump(exportParams *ExportParam) *MoDump {
	m := reuse.Alloc[MoDump](nil)
	m.ExportParams = exportParams
	return m
}

func (node *MoDump) Format(ctx *FmtCtx) {
	ctx.WriteString("modump")
	ctx.WriteString(" query_result")
	ctx.WriteByte(' ')
	ctx.WriteString(node.ExportParams.QueryId)
	ctx.WriteByte(' ')
	node.ExportParams.format(ctx, false)
}

func (node *MoDump) GetStatementType() string { return "MoDump" }

func (node *MoDump) GetQueryType() string { return QueryTypeDQL }

func (node MoDump) TypeName() string { return "tree.MoDump" }

func (node *MoDump) Free() {
	reuse.Free[MoDump](node, nil)
}

func (node *MoDump) reset() {
	// if node.ExportParams != nil {
	// 	reuse.Free[ExportParam](node.ExportParams, nil)
	// }
	*node = MoDump{}
}
