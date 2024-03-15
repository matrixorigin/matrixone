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

import "github.com/matrixorigin/matrixone/pkg/common/reuse"

func init() {
	reuse.CreatePool[BackupStart](
		func() *BackupStart { return &BackupStart{} },
		func(b *BackupStart) { b.reset() },
		reuse.DefaultOptions[BackupStart](), //.
	) //WithEnableChecker()
}

type BackupStart struct {
	statementImpl
	Timestamp   string
	IsS3        bool
	Dir         string
	Parallelism string
	// s3 option
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

func NewBackupStart(timestamp string, isS3 bool, dir string, parallelism string, option []string) *BackupStart {
	backup := reuse.Alloc[BackupStart](nil)
	backup.Timestamp = timestamp
	backup.IsS3 = isS3
	backup.Dir = dir
	backup.Parallelism = parallelism
	backup.Option = option
	return backup
}

func (node *BackupStart) GetStatementType() string { return "Backup Start" }

func (node *BackupStart) GetQueryType() string { return QueryTypeOth }

func (node BackupStart) TypeName() string { return "tree.BackupStart" }

func (node *BackupStart) reset() {
	if node.Option != nil {
		node.Option = nil
	}
	*node = BackupStart{}
}

func (node *BackupStart) Free() {
	reuse.Free[BackupStart](node, nil)
}
