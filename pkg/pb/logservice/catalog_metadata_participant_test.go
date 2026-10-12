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

package logservice

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCatalogMetadataParticipantRejectsInvalidObservation(t *testing.T) {
	valid := CatalogMetadataBarrier{RecipientGeneration: 7, MembershipEpoch: 3, RequiredGeneration: 5, Phase: CATALOG_METADATA_BARRIER_SEALED}
	cases := []struct {
		name       string
		generation uint64
		mutate     func(*CatalogMetadataBarrier)
	}{
		{"zero generation", 0, func(b *CatalogMetadataBarrier) { b.RecipientGeneration = 0 }},
		{"missing recipient", 7, func(b *CatalogMetadataBarrier) { b.RecipientGeneration = 0 }},
		{"stale recipient", 7, func(b *CatalogMetadataBarrier) { b.RecipientGeneration = 6 }},
		{"future recipient", 7, func(b *CatalogMetadataBarrier) { b.RecipientGeneration = 8 }},
		{"zero epoch", 7, func(b *CatalogMetadataBarrier) { b.MembershipEpoch = 0 }},
		{"zero requirement", 7, func(b *CatalogMetadataBarrier) { b.RequiredGeneration = 0 }},
		{"disabled", 7, func(b *CatalogMetadataBarrier) { b.Phase = CATALOG_METADATA_BARRIER_DISABLED }},
		{"negative phase", 7, func(b *CatalogMetadataBarrier) { b.Phase = -1 }},
		{"unknown phase", 7, func(b *CatalogMetadataBarrier) { b.Phase = CATALOG_METADATA_BARRIER_ACTIVATED + 1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var p CatalogMetadataParticipant
			p.Observe(7, nil)
			require.Nil(t, p.Ack(7))
			invalid := valid
			tc.mutate(&invalid)
			p.Observe(tc.generation, &invalid)
			require.Nil(t, p.Ack(tc.generation))
			p.Observe(7, &valid)
			before := p.Ack(7)
			p.Observe(tc.generation, &invalid)
			p.Observe(7, nil)
			require.Equal(t, before, p.Ack(7))
		})
	}
}

func TestCatalogMetadataParticipantMonotonicObservation(t *testing.T) {
	cases := []struct {
		name                        string
		generation, epoch, required uint64
		phase                       CatalogMetadataBarrierPhase
		accept                      bool
	}{
		{"duplicate", 7, 3, 5, CATALOG_METADATA_BARRIER_SEALED, true},
		{"next phase", 7, 3, 5, CATALOG_METADATA_BARRIER_ACTIVATED, true},
		{"stale phase", 7, 3, 5, CATALOG_METADATA_BARRIER_PREPARING, false},
		{"stale generation", 6, 4, 6, CATALOG_METADATA_BARRIER_ACTIVATED, false},
		{"stale epoch", 7, 2, 6, CATALOG_METADATA_BARRIER_ACTIVATED, false},
		{"stale requirement", 7, 4, 4, CATALOG_METADATA_BARRIER_ACTIVATED, false},
		{"new epoch", 7, 4, 5, CATALOG_METADATA_BARRIER_PREPARING, true},
		{"new requirement", 7, 3, 6, CATALOG_METADATA_BARRIER_PREPARING, true},
		{"new generation", 8, 3, 5, CATALOG_METADATA_BARRIER_SEALED, true},
		{"new generation cannot lower epoch", 8, 2, 6, CATALOG_METADATA_BARRIER_ACTIVATED, false},
		{"new generation cannot lower requirement", 8, 4, 4, CATALOG_METADATA_BARRIER_ACTIVATED, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var p CatalogMetadataParticipant
			p.Observe(7, &CatalogMetadataBarrier{RecipientGeneration: 7, MembershipEpoch: 3, RequiredGeneration: 5, Phase: CATALOG_METADATA_BARRIER_SEALED})
			before := p.Ack(7)
			require.Nil(t, p.Ack(0))
			require.Nil(t, p.Ack(6))
			require.Nil(t, p.Ack(8))
			p.Observe(tc.generation, &CatalogMetadataBarrier{RecipientGeneration: tc.generation, MembershipEpoch: tc.epoch, RequiredGeneration: tc.required, Phase: tc.phase})
			if !tc.accept {
				require.Equal(t, before, p.Ack(7))
				return
			}
			require.Equal(t, &CatalogMetadataAck{Generation: tc.generation, MembershipEpoch: tc.epoch, RequiredGeneration: tc.required, ObservedPhase: tc.phase}, p.Ack(tc.generation))
			if tc.generation != 7 {
				require.Nil(t, p.Ack(7))
			}
		})
	}
}

func TestCatalogMetadataParticipantObservationDoesNotSealOrAlias(t *testing.T) {
	var p CatalogMetadataParticipant
	for phase := CATALOG_METADATA_BARRIER_PREPARING; phase <= CATALOG_METADATA_BARRIER_ACTIVATED; phase++ {
		snapshot := CatalogMetadataBarrier{RecipientGeneration: 7, MembershipEpoch: 3, RequiredGeneration: 5, CompletedGeneration: 5, Phase: phase, Admitted: true, MetadataReadsEnabled: true}
		p.Observe(7, &snapshot)
		expected := &CatalogMetadataAck{Generation: 7, MembershipEpoch: 3, RequiredGeneration: 5, ObservedPhase: phase}
		require.Equal(t, expected, p.Ack(7))
		snapshot = CatalogMetadataBarrier{}
		require.Equal(t, expected, p.Ack(7), "观察后修改输入不得改变已发布 ack")
		ack := p.Ack(7)
		*ack = CatalogMetadataAck{Generation: 99, MembershipEpoch: 99, RequiredGeneration: 99, SealComplete: true}
		require.Equal(t, expected, p.Ack(7), "调用者修改返回值不得污染内部状态或制造 seal")
	}
}

func TestCatalogMetadataParticipantConcurrentObservation(t *testing.T) {
	var p CatalogMetadataParticipant
	p.Observe(7, &CatalogMetadataBarrier{RecipientGeneration: 7, MembershipEpoch: 1, RequiredGeneration: 1, Phase: CATALOG_METADATA_BARRIER_PREPARING})
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, epoch := range []uint64{1, 2} {
		for _, phase := range []CatalogMetadataBarrierPhase{CATALOG_METADATA_BARRIER_PREPARING, CATALOG_METADATA_BARRIER_ACTIVATED} {
			wg.Go(func() {
				<-start
				p.Observe(7, &CatalogMetadataBarrier{RecipientGeneration: 7, MembershipEpoch: epoch, RequiredGeneration: epoch, Phase: phase})
				// 与其他观察者并发修改返回副本，race 检查可识别别名发布。
				ack := p.Ack(7)
				ack.SealComplete = true
				ack.MembershipEpoch = 99
			})
		}
	}
	close(start)
	wg.Wait()
	require.Equal(t, &CatalogMetadataAck{Generation: 7, MembershipEpoch: 2, RequiredGeneration: 2, ObservedPhase: CATALOG_METADATA_BARRIER_ACTIVATED}, p.Ack(7))
}
