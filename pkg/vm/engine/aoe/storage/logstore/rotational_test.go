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

package logstore

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRotation(t *testing.T) {
	dir := "/tmp/testrotation"
	os.RemoveAll(dir)
	rot, err := OpenRotational(
		dir,
		"store",
		".rot",
		nil,
		&MaxSizeRotationChecker{MaxSize: 10}, nil)
	assert.Nil(t, err)

	history := rot.GetHistory()
	assert.True(t, history.Empty())

	var bs bytes.Buffer
	bs.WriteString("Hello ")
	bs.WriteString("World")
	err = rot.PrepareWrite(len(bs.Bytes()))
	assert.NotNil(t, err)

	bs.Reset()
	bs.WriteString("Hi")
	bs.WriteString("World")
	err = rot.PrepareWrite(len(bs.Bytes()))
	assert.Nil(t, err)
	_, err = rot.Write(bs.Bytes())
	assert.Nil(t, err)

	bs.Reset()
	bs.WriteString("Hello")
	bs.WriteString("World")
	err = rot.PrepareWrite(len(bs.Bytes()))
	assert.Nil(t, err)
	_, err = rot.Write(bs.Bytes())
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), rot.currVersion)
	err = rot.Close()
	assert.Nil(t, err)

	rot, err = OpenRotational("/tmp/testrotation", "store", ".rot", nil, &MaxSizeRotationChecker{MaxSize: 10}, nil)
	assert.Nil(t, err)
	assert.NotNil(t, rot)
	defer rot.Close()

	bs.Reset()
	bs.WriteString("hi,matrix")
	err = rot.PrepareWrite(len(bs.Bytes()))
	assert.Nil(t, err)
	_, err = rot.Write(bs.Bytes())
	assert.Nil(t, err)
	t.Log(rot.String())

	err = rot.Sync()
	assert.Nil(t, err)
}

func TestParse(t *testing.T) {
	_, err := ParseVersion("stsss-9.dd", "dsd", "dk")
	assert.NotNil(t, err)
	v, err := ParseVersion("stsss-9.dd", "stsss", ".dd")
	assert.Nil(t, err)
	assert.Equal(t, uint64(9), v)
	_, err = ParseVersion("stsss-s.dd", "stsss", ".dd")
	assert.NotNil(t, err)
}

func TestVersionsMeta(t *testing.T) {
	hub := newArchivedHub(nil)
	v := &versionInfo{
		id: common.GetGlobalSeqNum(),
		commit: common.Range{
			Left:  0,
			Right: 100,
		},
		hub: hub,
	}

	v.Archive()
	err := hub.TryTruncate(nil)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(hub.versions))

	v = &versionInfo{
		id: common.GetGlobalSeqNum(),
		commit: common.Range{
			Left:  101,
			Right: 200,
		},
		checkpoint: &common.Range{
			Left:  0,
			Right: 150,
		},
		hub: hub,
	}
	v.Archive()

	cbCalled := false
	cb := func(id uint64) {
		cbCalled = true
		assert.Equal(t, v.id, id)
	}
	err = hub.TryTruncate(cb)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(hub.versions))
	assert.True(t, cbCalled)

	v = &versionInfo{
		id: common.GetGlobalSeqNum(),
		commit: common.Range{
			Left:  201,
			Right: 300,
		},
		checkpoint: &common.Range{
			Left:  151,
			Right: 180,
		},
		hub: hub,
	}
	v.Archive()
	err = hub.TryTruncate(cb)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(hub.versions))

	v = &versionInfo{
		id: common.GetGlobalSeqNum(),
		commit: common.Range{
			Left:  201,
			Right: 299,
		},
		checkpoint: &common.Range{
			Left:  181,
			Right: 190,
		},
		hub: hub,
	}
	err = v.AppendCommit(200)
	assert.NotNil(t, err)
	err = v.AppendCommit(300)
	assert.Nil(t, err)
	err = v.UnionCheckpointRange(common.Range{Left: 181, Right: 199})
	assert.Nil(t, err)
	v.Archive()
	err = hub.TryTruncate(cb)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(hub.versions))

	v = &versionInfo{
		id: common.GetGlobalSeqNum(),
		commit: common.Range{
			Left:  301,
			Right: 400,
		},
		checkpoint: &common.Range{
			Left:  100,
			Right: 400,
		},
		hub: hub,
	}

	v.Archive()
	err = hub.TryTruncate(cb)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(hub.versions))

	v = &versionInfo{
		id: common.GetGlobalSeqNum(),
		commit: common.Range{
			Left:  401,
			Right: 500,
		},
		checkpoint: &common.Range{
			Left:  0,
			Right: 400,
		},
		hub: hub,
	}

	v.Archive()
	err = hub.TryTruncate(cb)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(hub.versions))
}

func TestTruncate(t *testing.T) {

}
