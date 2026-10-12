// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestProxyHeartbeatCatalogMetadataParticipant(t *testing.T) {
	client := &testHAClient{}
	s := &Server{haKeeperClient: client, configData: util.NewConfigData(nil), runtime: runtime.DefaultRuntime(),
		viewMetadataAdmissionGeneration: 7, viewMetadataAdmissionUpdated: make(chan struct{}, 1)}
	s.config.HAKeeper.HeartbeatTimeout.Duration = time.Second
	var sent pb.ProxyHeartbeat
	batch := pb.CommandBatch{}
	var sendErr error
	client.heartbeatFn = func(_ context.Context, hb pb.ProxyHeartbeat) (pb.CommandBatch, error) {
		sent = hb
		return batch, sendErr
	}
	// 保持旧 admission 关闭；新 barrier 的读取标记不能替代它。
	legacy := &pb.ViewMetadataAdmission{Generation: 7, Preparing: true}
	batch.ViewMetadataAdmission = legacy
	barrier := &pb.CatalogMetadataBarrier{RecipientGeneration: 7, MembershipEpoch: 3, RequiredGeneration: 5,
		Phase: pb.CATALOG_METADATA_BARRIER_DISABLED, Admitted: true, MetadataReadsEnabled: true}
	batch.CatalogMetadataBarrier = barrier
	s.doHeartbeat(context.Background())
	require.Equal(t, &pb.CatalogMetadataCapabilities{BarrierParticipantProtocol: 1}, sent.CatalogMetadataCapabilities)

	require.True(t, sent.ViewMetadataAdmissionSupported)
	require.Equal(t, uint64(7), sent.ViewMetadataAdmissionGeneration)
	require.Nil(t, sent.CatalogMetadataAck)
	require.Nil(t, s.catalogMetadataParticipant.Ack(7))
	require.Equal(t, legacy, s.viewMetadataAdmission.Load())

	for phase := pb.CATALOG_METADATA_BARRIER_PREPARING; phase <= pb.CATALOG_METADATA_BARRIER_ACTIVATED; phase++ {
		barrier.Phase = phase
		before := s.catalogMetadataParticipant.Ack(7)
		s.doHeartbeat(context.Background())
		require.Equal(t, before, sent.CatalogMetadataAck, "本次请求只能携带先前已观察到的 ack")
		expected := &pb.CatalogMetadataAck{Generation: 7, MembershipEpoch: 3, RequiredGeneration: 5, ObservedPhase: phase}
		require.Equal(t, expected, s.catalogMetadataParticipant.Ack(7))
		s.doHeartbeat(context.Background())
		require.Equal(t, expected, sent.CatalogMetadataAck, "响应必须在下一次真实 heartbeat 中确认")
		require.Equal(t, legacy, s.viewMetadataAdmission.Load())
	}

	// 旧 admission 刷新失败，不得撤销已经观察到的新 barrier。
	batch.ViewMetadataAdmission = &pb.ViewMetadataAdmission{Generation: 7, Epoch: 1, Preparing: true}
	barrier.RequiredGeneration = 6
	s.doHeartbeat(context.Background())
	require.Equal(t, &pb.CatalogMetadataAck{Generation: 7, MembershipEpoch: 3, RequiredGeneration: 6,
		ObservedPhase: pb.CATALOG_METADATA_BARRIER_ACTIVATED}, s.catalogMetadataParticipant.Ack(7))
	require.Equal(t, legacy, s.viewMetadataAdmission.Load())
	batch.ViewMetadataAdmission = legacy

	// RPC 报错时即使同时返回较新的快照，也不得推进 ack。
	before := s.catalogMetadataParticipant.Ack(7)
	barrier.MembershipEpoch++
	sendErr = errors.New("heartbeat failed")
	s.doHeartbeat(context.Background())
	require.Equal(t, before, s.catalogMetadataParticipant.Ack(7))
	require.Equal(t, legacy, s.viewMetadataAdmission.Load())
	sendErr = nil

	// 模拟新 incarnation；旧 ack 不能出现在新 generation 的请求中。
	s.viewMetadataAdmissionGeneration = 8
	s.doHeartbeat(context.Background())
	require.Equal(t, uint64(8), sent.ViewMetadataAdmissionGeneration)
	require.Nil(t, sent.CatalogMetadataAck)
	require.Nil(t, s.catalogMetadataParticipant.Ack(8))
	barrier.RecipientGeneration = 9
	s.doHeartbeat(context.Background())
	require.Nil(t, sent.CatalogMetadataAck)
	require.Nil(t, s.catalogMetadataParticipant.Ack(8), "未来 recipient 同样不能污染当前 incarnation")
	barrier.RecipientGeneration = 8
	s.doHeartbeat(context.Background())
	require.Nil(t, sent.CatalogMetadataAck)
	s.doHeartbeat(context.Background())
	require.Equal(t, &pb.CatalogMetadataAck{Generation: 8, MembershipEpoch: 4, RequiredGeneration: 6, ObservedPhase: pb.CATALOG_METADATA_BARRIER_ACTIVATED}, sent.CatalogMetadataAck)
	require.Equal(t, legacy, s.viewMetadataAdmission.Load())

	// 旧 HAKeeper 不携带 admission 时，仍保留原有兼容回退行为。
	before = s.catalogMetadataParticipant.Ack(8)
	batch = pb.CommandBatch{}
	s.doHeartbeat(context.Background())
	require.Equal(t, &pb.ViewMetadataAdmission{Ready: true, Admitted: true}, s.viewMetadataAdmission.Load())
	require.Equal(t, before, sent.CatalogMetadataAck)
	require.Equal(t, before, s.catalogMetadataParticipant.Ack(8))

	// 本地 generation 尚未初始化时，新能力广告不依赖旧 admission 的支持位。
	s.viewMetadataAdmissionGeneration = 0
	s.doHeartbeat(context.Background())
	require.False(t, sent.ViewMetadataAdmissionSupported)
	require.Nil(t, sent.CatalogMetadataAck)
	require.Equal(t, &pb.CatalogMetadataCapabilities{BarrierParticipantProtocol: 1}, sent.CatalogMetadataCapabilities)
}
