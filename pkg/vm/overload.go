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

package vm

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergecte"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergerecursive"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/shuffle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/stream"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/window"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/anti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/insert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/join"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/left"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopleft"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopmark"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopsingle"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mark"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeblock"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergedelete"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergelimit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeoffset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/onduplicatekey"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsertunique"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/right"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightanti"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/rightsemi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/semi"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/single"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type vft struct {
	fnString  func(*Instruction, *bytes.Buffer)
	fnPrepare func(*Instruction, *process.Process) error
	fnCall    func(*Instruction, *process.Process) (*batch.Batch, error)
	// I think we are mssing a fnStart call.  For NestedLoopJoin, for example,
	// we want to call fnStart for next iteration of the inner loop.  This is
	// For index nestloop join, restart is the right place to set
	// next index range.
}

var InstructionVFT = [...]vft{
	Top:         {top.String, top.Prepare, top.Call},
	Join:        {join.String, join.Prepare, join.Call},
	Semi:        {semi.String, semi.Prepare, semi.Call},
	RightSemi:   {rightsemi.String, rightsemi.Prepare, rightsemi.Call},
	RightAnti:   {rightanti.String, rightanti.Prepare, rightanti.Call},
	Left:        {left.String, left.Prepare, left.Call},
	Right:       {right.String, right.Prepare, right.Call},
	Single:      {single.String, single.Prepare, single.Call},
	Limit:       {limit.String, limit.Prepare, limit.Call},
	Order:       {order.String, order.Prepare, order.Call},
	Group:       {group.String, group.Prepare, group.Call},
	Window:      {window.String, window.Prepare, window.Call},
	Merge:       {merge.String, merge.Prepare, merge.Call},
	Output:      {output.String, output.Prepare, output.Call},
	Offset:      {offset.String, offset.Prepare, offset.Call},
	Product:     {product.String, product.Prepare, product.Call},
	Restrict:    {restrict.String, restrict.Prepare, restrict.Call},
	Dispatch:    {dispatch.String, dispatch.Prepare, dispatch.Call},
	Connector:   {connector.String, connector.Prepare, connector.Call},
	Projection:  {projection.String, projection.Prepare, projection.Call},
	Anti:        {anti.String, anti.Prepare, anti.Call},
	Mark:        {mark.String, mark.Prepare, mark.Call},
	MergeBlock:  {mergeblock.String, mergeblock.Prepare, mergeblock.Call},
	MergeDelete: {mergedelete.String, mergedelete.Prepare, mergedelete.Call},
	LoopJoin:    {loopjoin.String, loopjoin.Prepare, loopjoin.Call},
	LoopLeft:    {loopleft.String, loopleft.Prepare, loopleft.Call},
	LoopSingle:  {loopsingle.String, loopsingle.Prepare, loopsingle.Call},
	LoopSemi:    {loopsemi.String, loopsemi.Prepare, loopsemi.Call},
	LoopAnti:    {loopanti.String, loopanti.Prepare, loopanti.Call},
	LoopMark:    {loopmark.String, loopmark.Prepare, loopmark.Call},

	MergeTop:       {mergetop.String, mergetop.Prepare, mergetop.Call},
	MergeLimit:     {mergelimit.String, mergelimit.Prepare, mergelimit.Call},
	MergeOrder:     {mergeorder.String, mergeorder.Prepare, mergeorder.Call},
	MergeGroup:     {mergegroup.String, mergegroup.Prepare, mergegroup.Call},
	MergeOffset:    {mergeoffset.String, mergeoffset.Prepare, mergeoffset.Call},
	MergeRecursive: {mergerecursive.String, mergerecursive.Prepare, mergerecursive.Call},
	MergeCTE:       {mergecte.String, mergecte.Prepare, mergecte.Call},

	Deletion:        {deletion.String, deletion.Prepare, deletion.Call},
	Insert:          {insert.String, insert.Prepare, insert.Call},
	OnDuplicateKey:  {onduplicatekey.String, onduplicatekey.Prepare, onduplicatekey.Call},
	PreInsert:       {preinsert.String, preinsert.Prepare, preinsert.Call},
	PreInsertUnique: {preinsertunique.String, preinsertunique.Prepare, preinsertunique.Call},
	External:        {external.String, external.Prepare, external.Call},

	Minus:        {minus.String, minus.Prepare, minus.Call},
	Intersect:    {intersect.String, intersect.Prepare, intersect.Call},
	IntersectAll: {intersectall.String, intersectall.Prepare, intersectall.Call},

	HashBuild: {hashbuild.String, hashbuild.Prepare, hashbuild.Call},

	TableFunction: {table_function.String, table_function.Prepare, table_function.Call},

	LockOp: {lockop.String, lockop.Prepare, lockop.Call},

	Shuffle: {shuffle.String, shuffle.Prepare, shuffle.Call},
	Stream:  {stream.String, stream.Prepare, stream.Call},
}

func InstructionCall(ins *Instruction, proc *process.Process) (*batch.Batch, error) {
	return InstructionVFT[ins.Op].fnCall(ins, proc)
}
