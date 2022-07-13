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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func TestHAKeeperClientsCanBeCreated(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{testServiceAddress},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c1, err := NewCNHAKeeperClient(ctx, cfg)
		require.NoError(t, err)
		assert.NoError(t, c1.Close())
		c2, err := NewDNHAKeeperClient(ctx, cfg)
		assert.NoError(t, err)
		assert.NoError(t, c2.Close())
		c3, err := NewLogHAKeeperClient(ctx, cfg)
		assert.NoError(t, err)
		assert.NoError(t, c3.Close())
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientCanNotConnectToNonHAKeeperNode(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{testServiceAddress},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := NewCNHAKeeperClient(ctx, cfg)
		require.Equal(t, ErrNoHAKeeper, err)
		_, err = NewDNHAKeeperClient(ctx, cfg)
		assert.Equal(t, ErrNoHAKeeper, err)
		_, err = NewLogHAKeeperClient(ctx, cfg)
		assert.Equal(t, ErrNoHAKeeper, err)
	}
	runServiceTest(t, false, true, fn)
}

func TestHAKeeperClientSendCNHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{testServiceAddress},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c1, err := NewCNHAKeeperClient(ctx, cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c1.Close())
		}()
		hb := pb.CNStoreHeartbeat{
			UUID:           s.ID(),
			ServiceAddress: "addr1",
		}
		require.NoError(t, c1.SendCNHeartbeat(ctx, hb))

		c2, err := NewDNHAKeeperClient(ctx, cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c2.Close())
		}()
		hb2 := pb.DNStoreHeartbeat{
			UUID:           s.ID(),
			ServiceAddress: "addr2",
		}
		cb, err := c2.SendDNHeartbeat(ctx, hb2)
		require.NoError(t, err)
		assert.Equal(t, 0, len(cb.Commands))

		cd, err := c1.GetClusterDetails(ctx)
		require.NoError(t, err)
		cn := pb.CNNode{
			UUID:           s.ID(),
			ServiceAddress: "addr1",
		}
		dn := pb.DNNode{
			UUID:           s.ID(),
			ServiceAddress: "addr2",
		}
		assert.Equal(t, []pb.CNNode{cn}, cd.CNNodes)
		assert.Equal(t, []pb.DNNode{dn}, cd.DNNodes)
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientSendDNHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{testServiceAddress},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c, err := NewDNHAKeeperClient(ctx, cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c.Close())
		}()
		hb := pb.DNStoreHeartbeat{
			UUID: s.ID(),
		}
		cb, err := c.SendDNHeartbeat(ctx, hb)
		require.NoError(t, err)
		assert.Equal(t, 0, len(cb.Commands))

		sc := pb.ScheduleCommand{
			UUID:        s.ID(),
			ServiceType: pb.DnService,
			ShutdownStore: &pb.ShutdownStore{
				StoreID: "hello world",
			},
		}
		require.NoError(t, s.store.addScheduleCommands(ctx, 0, []pb.ScheduleCommand{sc}))
		cb, err = c.SendDNHeartbeat(ctx, hb)
		require.NoError(t, err)
		require.Equal(t, 1, len(cb.Commands))
		require.Equal(t, sc, cb.Commands[0])
	}
	runServiceTest(t, true, true, fn)
}

func TestHAKeeperClientSendLogHeartbeat(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		cfg := HAKeeperClientConfig{
			ServiceAddresses: []string{testServiceAddress},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c, err := NewLogHAKeeperClient(ctx, cfg)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, c.Close())
		}()
		hb := s.store.getHeartbeatMessage()
		cb, err := c.SendLogHeartbeat(ctx, hb)
		require.NoError(t, err)
		assert.Equal(t, 0, len(cb.Commands))

		sc := pb.ScheduleCommand{
			UUID:        s.ID(),
			ServiceType: pb.DnService,
			ShutdownStore: &pb.ShutdownStore{
				StoreID: "hello world",
			},
		}
		require.NoError(t, s.store.addScheduleCommands(ctx, 0, []pb.ScheduleCommand{sc}))
		cb, err = c.SendLogHeartbeat(ctx, hb)
		require.NoError(t, err)
		require.Equal(t, 1, len(cb.Commands))
		require.Equal(t, sc, cb.Commands[0])
	}
	runServiceTest(t, true, true, fn)
}
