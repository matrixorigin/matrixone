// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

type BackupStart struct {
	statementImpl
	Timestamp   string
	IsS3        bool
	Dir         string
	Parallelism string
	//s3 option
	Option []string
}

func (node *BackupStart) Format(ctx *FmtCtx) {
	ctx.WriteString("backup ")
	ctx.WriteString(node.Timestamp)
	ctx.WriteString(" ")
	if node.IsS3 {
		ctx.WriteString("s3option ")
		formatS3option(ctx, node.Option)
	} else {
		ctx.WriteString("filesystem ")
		ctx.WriteString(node.Dir)
		ctx.WriteString(" parallelism ")
		ctx.WriteString(node.Parallelism)
	}
}

func (node *BackupStart) GetStatementType() string { return "Backup Start" }
func (node *BackupStart) GetQueryType() string     { return QueryTypeOth }
