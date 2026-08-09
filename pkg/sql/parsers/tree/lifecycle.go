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

type LifecycleAction uint8

const (
	LifecycleActionDelete LifecycleAction = iota + 1
	LifecycleActionArchive
)

type LifecycleOperation uint8

const (
	LifecycleOperationSet LifecycleOperation = iota + 1
	LifecycleOperationPause
	LifecycleOperationResume
	LifecycleOperationUnset
)

type LifecyclePolicy struct {
	Column             Identifier
	ExpireAfterDays    uint32
	Action             LifecycleAction
	Stage              Identifier
	HasStage           bool
	PurgeAfterDays     uint32
	HasPurgeAfter      bool
	LateArrivalDays    uint32
	EvaluationTimezone string
}

type AlterOptionLifecycle struct {
	alterOptionImpl
	Operation LifecycleOperation
	Policy    LifecyclePolicy
}

func NewAlterOptionLifecycle(operation LifecycleOperation, policy LifecyclePolicy) *AlterOptionLifecycle {
	return &AlterOptionLifecycle{Operation: operation, Policy: policy}
}

func (node *AlterOptionLifecycle) Free() {
	*node = AlterOptionLifecycle{}
}

func (node *AlterOptionLifecycle) Format(ctx *FmtCtx) {
	switch node.Operation {
	case LifecycleOperationSet:
		ctx.WriteString("set lifecycle (column ")
		node.Policy.Column.Format(ctx)
		ctx.WriteString(", expire after interval ")
		ctx.WriteString(formatUint(uint64(node.Policy.ExpireAfterDays)))
		ctx.WriteString(" day, action ")
		switch node.Policy.Action {
		case LifecycleActionDelete:
			ctx.WriteString("delete")
		case LifecycleActionArchive:
			ctx.WriteString("archive")
		}
		if node.Policy.HasStage {
			ctx.WriteString(", stage ")
			node.Policy.Stage.Format(ctx)
		}
		if node.Policy.HasPurgeAfter {
			ctx.WriteString(", purge eligible after interval ")
			ctx.WriteString(formatUint(uint64(node.Policy.PurgeAfterDays)))
			ctx.WriteString(" day")
		}
		ctx.WriteByte(')')
	case LifecycleOperationPause:
		ctx.WriteString("pause lifecycle")
	case LifecycleOperationResume:
		ctx.WriteString("resume lifecycle")
	case LifecycleOperationUnset:
		ctx.WriteString("unset lifecycle")
	}
}

func (node AlterOptionLifecycle) TypeName() string { return "tree.AlterOptionLifecycle" }

type ShowLifecycleKind uint8

const (
	ShowLifecycleBinding ShowLifecycleKind = iota + 1
	ShowLifecycleJobs
	ShowLifecycleDatasets
	ShowLifecycleRestores
)

type ShowLifecycle struct {
	statementImpl
	Kind  ShowLifecycleKind
	Table *TableName
	Page  *Limit
}

func (node *ShowLifecycle) Format(ctx *FmtCtx) {
	switch node.Kind {
	case ShowLifecycleBinding:
		ctx.WriteString("show lifecycle for table ")
		node.Table.Format(ctx)
	case ShowLifecycleJobs:
		ctx.WriteString("show lifecycle jobs")
	case ShowLifecycleDatasets:
		ctx.WriteString("show lifecycle datasets for table ")
		node.Table.Format(ctx)
	case ShowLifecycleRestores:
		ctx.WriteString("show lifecycle restores")
	}
	if node.Page != nil {
		ctx.WriteByte(' ')
		node.Page.Format(ctx)
	}
}

func (node *ShowLifecycle) Free()                    { *node = ShowLifecycle{} }
func (node ShowLifecycle) TypeName() string          { return "tree.ShowLifecycle" }
func (node *ShowLifecycle) GetStatementType() string { return "Show Lifecycle" }
func (node *ShowLifecycle) GetQueryType() string     { return QueryTypeDQL }

type RestoreArchiveDataset struct {
	statementImpl
	DatasetID string
	Target    *TableName
}

func (node *RestoreArchiveDataset) Format(ctx *FmtCtx) {
	ctx.WriteString("restore archive dataset ")
	ctx.WriteString("'" + node.DatasetID + "'")
	ctx.WriteString(" to table ")
	node.Target.Format(ctx)
}

func (node *RestoreArchiveDataset) Free()                    { *node = RestoreArchiveDataset{} }
func (node RestoreArchiveDataset) TypeName() string          { return "tree.RestoreArchiveDataset" }
func (node *RestoreArchiveDataset) GetStatementType() string { return "Restore Archive Dataset" }
func (node *RestoreArchiveDataset) GetQueryType() string     { return QueryTypeDDL }

// RestoreArchiveRange restores every archived row whose frozen Lifecycle
// column value is in the half-open interval [From, To). Dataset is an
// internal publication unit; Source is the user-facing table identity used to
// select the immutable Dataset set before the first restore side effect.
type RestoreArchiveRange struct {
	statementImpl
	Source *TableName
	From   string
	To     string
	Target *TableName
}

func (node *RestoreArchiveRange) Format(ctx *FmtCtx) {
	ctx.WriteString("restore archive table ")
	node.Source.Format(ctx)
	ctx.WriteString(" between '")
	ctx.WriteString(node.From)
	ctx.WriteString("' and '")
	ctx.WriteString(node.To)
	ctx.WriteString("' to table ")
	node.Target.Format(ctx)
}

func (node *RestoreArchiveRange) Free()                    { *node = RestoreArchiveRange{} }
func (node RestoreArchiveRange) TypeName() string          { return "tree.RestoreArchiveRange" }
func (node *RestoreArchiveRange) GetStatementType() string { return "Restore Archive Range" }
func (node *RestoreArchiveRange) GetQueryType() string     { return QueryTypeDDL }

type PurgeArchiveDataset struct {
	statementImpl
	DatasetID string
}

func (node *PurgeArchiveDataset) Format(ctx *FmtCtx) {
	ctx.WriteString("purge archive dataset ")
	ctx.WriteString("'" + node.DatasetID + "'")
}

func (node *PurgeArchiveDataset) Free()                    { *node = PurgeArchiveDataset{} }
func (node PurgeArchiveDataset) TypeName() string          { return "tree.PurgeArchiveDataset" }
func (node *PurgeArchiveDataset) GetStatementType() string { return "Purge Archive Dataset" }
func (node *PurgeArchiveDataset) GetQueryType() string     { return QueryTypeDDL }

func formatUint(value uint64) string {
	if value == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for value > 0 {
		pos--
		buf[pos] = byte(value%10) + '0'
		value /= 10
	}
	return string(buf[pos:])
}
