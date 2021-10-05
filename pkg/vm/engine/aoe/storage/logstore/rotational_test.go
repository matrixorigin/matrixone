package logstore

import (
	"bytes"
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
		&MaxSizeRotationChecker{MaxSize: 10})
	assert.Nil(t, err)

	history := rot.GetHistory()
	assert.True(t, history.Empty())

	var bs bytes.Buffer
	bs.WriteString("Hello ")
	bs.WriteString("World")
	_, err = rot.Write(bs.Bytes())
	assert.NotNil(t, err)
	assert.True(t, history.Empty())

	bs.Reset()
	bs.WriteString("Hi")
	bs.WriteString("World")
	_, err = rot.Write(bs.Bytes())
	assert.Nil(t, err)

	bs.Reset()
	bs.WriteString("Hello")
	bs.WriteString("World")
	_, err = rot.Write(bs.Bytes())
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), rot.currVersion)
	err = rot.Close()
	assert.Nil(t, err)

	rot, err = OpenRotational("/tmp/testrotation", "store", ".rot", nil, &MaxSizeRotationChecker{MaxSize: 10})
	assert.Nil(t, err)
	assert.NotNil(t, rot)
	defer rot.Close()

	bs.Reset()
	bs.WriteString("hi,matrix")
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
