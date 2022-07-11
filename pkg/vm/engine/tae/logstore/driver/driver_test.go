package driver

import (
	"testing"

	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/stretchr/testify/assert"
)
var testServiceAddress="localhost:9000"

func startLogServiceServer()*logservice.Service{
	
	cfg:=logservice.Config{
		RTTMillisecond:       10,
		GossipSeedAddresses:  []string{"127.0.0.1:9000"},
		DeploymentID:         1,
		FS:                   vfs.NewStrictMem(),
		ServiceListenAddress: testServiceAddress,
		ServiceAddress:       testServiceAddress,
	}
	service, _ := logservice.NewService(cfg)
	return service
}

func startReplica(s *logservice.Service,shardID,replicaID uint64){
	init := make(map[uint64]string)
	init[2] = s.ID()
	s.GetStore().StartReplica(1, 2, init, false)
}

func TestAppendRead(t *testing.T) {
	s:=startLogServiceServer()
	startReplica(s,1,1)
	d:=NewLogServiceDriver(1,1,testServiceAddress)
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
             