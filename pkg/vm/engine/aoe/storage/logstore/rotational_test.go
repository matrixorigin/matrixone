package logstore

import (
	"bytes"
	"matrixone/pkg/vm/engine/aoe/storage/common"
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
		commit: Range{
			left:  0,
			right: 100,
		},
		hub: hub,
	}

	v.Archive()
	err := hub.TryTruncate(nil)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(hub.versions))

	v = &versionInfo{
		id: common.GetGlobalSeqNum(),
		commit: Range{
			left:  101,
			right: 200,
		},
		checkpoint: &Range{
			left:  0,
			right: 150,
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
		commit: Range{
			left:  201,
			right: 300,
		},
		checkpoint: &Range{
			left:  151,
			right: 180,
		},
		hub: hub,
	}
	v.Archive()
	err = hub.TryTruncate(cb)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(hub.versions))

	v = &versionInfo{
		id: common.GetGlobalSeqNum(),
		commit: Range{
			left:  201,
			right: 299,
		},
		checkpoint: &Range{
			left:  181,
			right: 190,
		},
		hub: hub,
	}
	err = v.AppendCommit(200)
	assert.NotNil(t, err)
	err = v.AppendCommit(300)
	assert.Nil(t, err)
	err = v.UnionCheckpointRange(Range{left: 181, right: 199})
	assert.Nil(t, err)
	v.Archive()
	err = hub.TryTruncate(cb)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(hub.versions))

	v = &versionInfo{
		id: common.GetGlobalSeqNum(),
		commit: Range{
			left:  301,
			right: 400,
		},
		checkpoint: &Range{
			left:  100,
			right: 400,
		},
		hub: hub,
	}

	v.Archive()
	err = hub.TryTruncate(cb)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(hub.versions))

	v = &versionInfo{
		id: common.GetGlobalSeqNum(),
		commit: Range{
			left:  401,
			right: 500,
		},
		checkpoint: &Range{
			left:  0,
			right: 400,
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
