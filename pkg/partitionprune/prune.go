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

package partitionprune

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Prune(
	proc *process.Process,
	bat *batch.Batch,
	metadata partition.PartitionMetadata,
) (partitionservice.PruneResult, error) {
	var bats []*batch.Batch
	for _, p := range metadata.Partitions {
		res, err := PrunePartitionByExpr(proc, bat, p)
		if err != nil {
			return partitionservice.PruneResult{}, err
		}
		bats = append(bats, res)
	}
	return partitionservice.NewPruneResult(bats, metadata.Partitions), nil
}

func PrunePartitionByExpr(
	proc *process.Process,
	bat *batch.Batch,
	partition partition.Partition,
) (*batch.Batch, error) {
	res := batch.NewWithSize(bat.VectorCount())
	for i := range bat.Vecs {
		res.Vecs[i] = vector.NewVec(*bat.Vecs[i].GetType())
	}

	executor, err := colexec.NewExpressionExecutor(proc, partition.Expr)
	if err != nil {
		return nil, err
	}
	defer executor.Free()

	vec, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return nil, err
	}
	chosen := vector.MustFixedColNoTypeCheck[bool](vec)
	for i, c := range chosen {
		if c {
			for j := range res.Vecs {
				err = res.Vecs[j].UnionOne(bat.Vecs[j], int64(i), proc.Mp())
				if err != nil {
					return nil, err
				}
			}
		}
	}
	res.SetRowCount(res.Vecs[0].Length())
	return res, nil
}
