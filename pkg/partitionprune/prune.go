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
	p "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Prune splits the input batch into multiple batches based on partition expressions.
// It is used when PrimaryKeysMayBeModified, during DELETE operations, or when inserting data.
// For each partition defined in the metadata, it evaluates the partition expression
// and creates a new batch containing only the rows that match that partition's criteria.
//
// Parameters:
//   - proc: The process context
//   - bat: The input batch to be partitioned
//   - metadata: Contains partition definitions and expressions
//   - partitionIndex: The column index used for partitioning (-1 if not applicable)
//
// Returns:
//   - PruneResult: Contains the split batches and their corresponding partition information
//   - error: Any error that occurred during the partitioning process
func Prune(
	proc *process.Process,
	bat *batch.Batch,
	metadata partition.PartitionMetadata,
	partitionIndex int32,
) (partitionservice.PruneResult, error) {
	// Initialize an array of batches, one for each partition
	bats := make([]*batch.Batch, len(metadata.Partitions))

	// Process each partition
	for i, p := range metadata.Partitions {
		// Evaluate the partition expression and get matching rows
		res, err := PrunePartitionByExpr(proc, bat, p, partitionIndex)
		if err != nil {
			return partitionservice.PruneResult{}, err
		}
		bats[i] = res
	}

	// Return the result containing all partitioned batches and their metadata
	return partitionservice.NewPruneResult(bats, metadata.Partitions), nil
}

// PrunePartitionByExpr evaluates a partition expression against the input batch and returns
// a new batch containing only the rows that match the partition criteria.
//
// The function creates a new batch with the same structure as the input batch and copies
// only the rows that satisfy the partition expression. If partitionIndex is specified,
// it adjusts the expression to use the correct column position.
//
// Parameters:
//   - proc: The process context
//   - bat: The input batch to be filtered
//   - partition: Contains the partition expression and metadata
//   - partitionIndex: The column index used for partitioning (-1 if not applicable)
//
// Returns:
//   - *batch.Batch: A new batch containing only the rows that match the partition criteria
//   - error: Any error that occurred during the evaluation
func PrunePartitionByExpr(
	proc *process.Process,
	bat *batch.Batch,
	partition partition.Partition,
	partitionIndex int32,
) (*batch.Batch, error) {
	// Create a new batch with the same structure as the input batch
	res := batch.NewWithSize(bat.VectorCount())
	for i := range bat.Vecs {
		res.Vecs[i] = vector.NewVec(*bat.Vecs[i].GetType())
	}

	// Handle partition expression
	expr := partition.Expr
	if partitionIndex != -1 {
		// If partitionIndex is specified, create a deep copy of the expression
		// and adjust column positions accordingly
		expr = p.DeepCopyExpr(expr)
		mustReplaceColPos(expr, partitionIndex)
	}

	// Create an expression executor to evaluate the partition expression
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	if err != nil {
		return nil, err
	}
	defer executor.Free()

	// Evaluate the expression against the input batch
	vec, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return nil, err
	}

	// Get the boolean results indicating which rows match the partition criteria
	chosen := vector.MustFixedColNoTypeCheck[bool](vec)

	// Copy matching rows to the result batch
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

	// Set the row count of the result batch
	res.SetRowCount(res.Vecs[0].Length())
	return res, nil
}
