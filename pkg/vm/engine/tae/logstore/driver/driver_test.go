package driver

import (
	"testing"

	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/stretchr/testify/assert"
)

func TestAppendRead(t *testing.T) {
	fs := vfs.NewStrictMem()
	service, ccfg, err := logservice.NewTestService(fs)
	assert.NoError(t, err)
	defer service.Close()
	d:=NewLogServiceDriver(1,1,&ccfg)
	t.Log(d.logServiceClient)
	e:=entry.GetBase()
	info:=entry.Info{
		Group: 11,
		GroupLSN: 1,
	}
	infoBuf:=info.Marshal()
	e.SetInfoBuf(infoBuf)
	lsn:=d.Append(e)
	e2:=d.Read(lsn)
	e2Info:=entry.Unmarshal(e2.GetInfoBuf())
	assert.Equal(t,uint64(11),e2Info.Group)
	assert.Equal(t,uint64(1),e2Info.GroupLSN)
	d.Close()
}
             