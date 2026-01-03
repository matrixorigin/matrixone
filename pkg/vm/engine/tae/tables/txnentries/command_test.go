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

package txnentries

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

func TestMergeBlocksCmd(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	droppedSid := objectio.NewSegmentid()
	createdSid := objectio.NewSegmentid()
	id1 := common.ID{TableID: 1}
	id1.SetSegmentID(droppedSid)
	id2 := common.ID{TableID: 1}
	id2.SetSegmentID(createdSid)
	droppedObjs := []*common.ID{&id1, &id1}
	createdObjs := []*common.ID{&id2}
	cmd := newMergeBlocksCmd(
		0,
		droppedObjs,
		createdObjs,
		nil,
		0)

	buf, err := cmd.MarshalBinary()
	assert.Nil(t, err)

	_, err = txnbase.BuildCommandFrom(buf)
	assert.Nil(t, err)
}

// errorMergeObjectsCmd is a mergeObjectsCmd that returns error on MarshalBinaryWithBuffer
type errorMergeObjectsCmd struct {
	mergeObjectsCmd
}

func (cmd *errorMergeObjectsCmd) MarshalBinaryWithBuffer(buf *bytes.Buffer) error {
	return moerr.NewInternalError(context.Background(), "marshal error")
}

func (cmd *errorMergeObjectsCmd) MarshalBinary() ([]byte, error) {
	poolBuf := txnbase.GetMarshalBuffer()
	err := cmd.MarshalBinaryWithBuffer(poolBuf)
	if err != nil {
		txnbase.PutMarshalBuffer(poolBuf)
		return nil, err
	}
	data := poolBuf.Bytes()
	result := make([]byte, len(data))
	copy(result, data)
	txnbase.PutMarshalBuffer(poolBuf)
	return result, nil
}

// TestMergeObjectsCmd_MarshalBinary_Error tests error branch in MarshalBinary
func TestMergeObjectsCmd_MarshalBinary_Error(t *testing.T) {
	defer testutils.AfterTest(t)()

	cmd := &errorMergeObjectsCmd{}
	buf, err := cmd.MarshalBinary()
	assert.Error(t, err)
	assert.Nil(t, buf)
}

// TestMergeObjectsCmd_MarshalBinary_LargeBuffer tests large buffer branch
func TestMergeObjectsCmd_MarshalBinary_LargeBuffer(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)

	droppedSid := objectio.NewSegmentid()
	createdSid := objectio.NewSegmentid()
	id1 := common.ID{TableID: 1}
	id1.SetSegmentID(droppedSid)
	id2 := common.ID{TableID: 1}
	id2.SetSegmentID(createdSid)
	droppedObjs := []*common.ID{&id1}
	createdObjs := []*common.ID{&id2}
	cmd := newMergeBlocksCmd(0, droppedObjs, createdObjs, nil, 0)

	// Get buffer and grow it beyond MaxPooledBufSize
	poolBuf := txnbase.GetMarshalBuffer()
	poolBuf.Grow(txnbase.MaxPooledBufSize + 1000)
	assert.Greater(t, poolBuf.Cap(), txnbase.MaxPooledBufSize)

	// Marshal with large buffer
	err := cmd.MarshalBinaryWithBuffer(poolBuf)
	assert.Nil(t, err)
	assert.Greater(t, poolBuf.Cap(), txnbase.MaxPooledBufSize)

	txnbase.PutMarshalBuffer(poolBuf)
}
