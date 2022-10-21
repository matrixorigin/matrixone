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

import "strconv"

type MoDump struct {
	statementImpl
	Database    Identifier
	Tables      TableNames
	OutFile     string
	MaxFileSize int64
}

func (node *MoDump) Format(ctx *FmtCtx) {
	ctx.WriteString("modump")
	if node.Database != "" {
		ctx.WriteString(" database ")
		ctx.WriteString(string(node.Database))
	}
	if node.Tables != nil {
		ctx.WriteString(" tables ")
		node.Tables.Format(ctx)
	}
	if node.OutFile != "" {
		ctx.WriteString(" into ")
		ctx.WriteString(node.OutFile)
	}
	if node.MaxFileSize != 0 {
		ctx.WriteString(" max_file_size ")
		ctx.WriteString(strconv.FormatInt(node.MaxFileSize, 10))
	}
}
