// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTestClient(t *testing.T) {
	fs := vfs.NewStrictMem()
	service, ccfg, err := NewTestService(fs)
	require.NoError(t, err)
	defer service.Close()

	// you need to decision what timeout value to use
	// you also need to retry requests on timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// get two clients, you can get many clients to be used concurrently
	// but each client itself is not goroutine safe
	client1, err := NewClient(ctx, "", ccfg)
	require.NoError(t, err)
	defer client1.Close()
	client2, err := NewClient(ctx, "", ccfg)
	require.NoError(t, err)
	defer client2.Close()

	// Don't use the Data field unless you know what you are doing
	rec1 := client1.GetLogRecord(5)
	rec2 := client2.GetLogRecord(5)
	copy(rec1.Payload(), []byte("hello"))
	copy(rec2.Payload(), []byte("world"))

	// append the records
	lsn1, err := client1.Append(ctx, rec1)
	require.NoError(t, err)
	lsn2, err := client2.Append(ctx, rec2)
	require.NoError(t, err)
	assert.Equal(t, lsn1+1, lsn2)

	// read them back and check whether the returned data is expected
	recs, lsn, err := client1.Read(ctx, lsn1, 1024)
	require.NoError(t, err)
	assert.Equal(t, lsn1, lsn)
	assert.Equal(t, 2, len(recs))
	assert.Equal(t, rec1.Payload(), recs[0].Payload())
	assert.Equal(t, rec2.Payload(), recs[1].Payload())

	client3 := NewStandbyClientWithRetry(ctx, "", ccfg)
	defer client3.Close()

	//let ValidateStandbyClient return error
	ccfg.LogShardID = 0
	client4 := NewStandbyClientWithRetry(ctx, "", ccfg)
	assert.Nil(t, client4)
}

func TestNewTestServicesUseIndependentPorts(t *testing.T) {
	service1, ccfg1, err := NewTestService(vfs.NewStrictMem())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, service1.Close())
	})

	service2, ccfg2, err := NewTestService(vfs.NewStrictMem())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, service2.Close())
	})

	require.NotEqual(t, ccfg1.ServiceAddresses, ccfg2.ServiceAddresses)
}

func TestNewTestServiceRetriesGossipPortCollisionWithStableIdentity(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, listener.Close())
	})
	occupiedPort := listener.Addr().(*net.TCPAddr).Port

	baseGenerator := newTestServiceConfigGenerator(vfs.NewStrictMem())
	var generatedIDs []string
	attempts := 0
	service, ccfg, err := newTestService(func() Config {
		cfg := baseGenerator()
		generatedIDs = append(generatedIDs, cfg.UUID)
		attempts++
		if attempts == 1 {
			cfg.GossipPort = occupiedPort
			cfg.GossipSeedAddresses = []string{getTestGossipAddress(occupiedPort)}
		}
		return cfg
	})
	require.NoError(t, err)
	require.NotNil(t, service)
	t.Cleanup(func() {
		require.NoError(t, service.Close())
	})

	require.GreaterOrEqual(t, attempts, 2)
	require.NotZero(t, ccfg.LogShardID)
	require.NotEmpty(t, ccfg.ServiceAddresses)
	for _, generatedID := range generatedIDs {
		require.Equal(t, service.ID(), generatedID)
	}
}
