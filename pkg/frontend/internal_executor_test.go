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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestIe(t *testing.T) {
	//pu := config.NewParameterUnit(&config.GlobalSystemVariables, config.HostMmu, config.Mempool, config.StorageEngine, config.ClusterNodes, config.ClusterCatalog)
	//executor := NewIternalExecutor(pu)
	//executor.ApplySessionOverride(ie.NewOptsBuilder().Username("test").Finish())
	//sess := executor.newCmdSession(ie.NewOptsBuilder().Database("testdb").Internal(true).Finish())
	//assert.Equal(t, "test", sess.GetMysqlProtocol().GetUserName())
	//
	//err := executor.Exec("select a from testtable", ie.NewOptsBuilder().Finish())
	//assert.Error(t, err)
}

func TestProtoMoreCoverage(t *testing.T) {
	pu := config.NewParameterUnit(&config.GlobalSystemVariables, config.HostMmu, config.Mempool, config.StorageEngine, config.ClusterNodes, config.ClusterCatalog)
	executor := NewIternalExecutor(pu)
	p := executor.proto
	assert.True(t, p.IsEstablished())
	p.SetEstablished()
	p.Quit()
	p.PrepareBeforeProcessingResultSet()
	_ = p.GetStats()
	assert.Panics(t, func() { p.GetRequest([]byte{1}) })
	assert.Panics(t, func() { p.Peer() })
	assert.Nil(t, p.SendResponse(nil))
	assert.Nil(t, p.SendResultSetTextBatchRow(nil, 0))
	assert.Nil(t, p.SendResultSetTextBatchRowSpeedup(nil, 0))
	assert.Nil(t, p.SendColumnDefinitionPacket(nil, 1))
	assert.Nil(t, p.SendColumnCountPacket(1))
	assert.Nil(t, p.SendEOFPacketIf(0, 1))
	assert.Nil(t, p.sendOKPacket(1, 1, 0, 0, ""))
	assert.Nil(t, p.sendEOFOrOkPacket(0, 1))
}
