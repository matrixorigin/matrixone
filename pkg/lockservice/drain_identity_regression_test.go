// Copyright 2026 Matrix Origin
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

package lockservice

import (
	"testing"
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

func TestDrainLateRetirementMustNotAuthorizeLiveIncarnation(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		oldID, newID := "1234567890123456789uuid1", "1234567890123456790uuid1"
		old := a.registerService(oldID)
		current := a.registerService(newID)
		current.setStatus(pb.Status_ServiceLockEnable)
		old.setStatus(pb.Status_ServiceCanRestart)
		// Exact order, no map-iteration oracle: new registers before old retires.
		a.disableTableBinds(old)
		if a.canRestartService(newID) {
			t.Fatal("old UUID retirement authorized the live undrained full service ID")
		}
	})
}

func TestDrainAmbiguousUUIDFailsClosed(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		old := a.registerService("1234567890123456789uuid1")
		current := a.registerService("1234567890123456790uuid1")
		old.setStatus(pb.Status_ServiceCanRestart)
		if a.canRestartService("uuid1") || a.setRestartService("uuid1") {
			t.Fatal("ambiguous UUID must not select an incarnation")
		}
		if !current.isStatus(pb.Status_ServiceLockEnable) {
			t.Fatal("ambiguous request changed the current incarnation")
		}
	})
}

func TestDrainLateStatusMustNotRegress(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		b := a.registerService("s1")
		b.setStatus(pb.Status_ServiceCanRestart)
		b.setStatus(pb.Status_ServiceLockWaiting)
		if !b.isStatus(pb.Status_ServiceCanRestart) {
			t.Fatal("late heartbeat regressed terminal drain status")
		}
	})
}

func TestDrainDuplicateRequestMustNotRegressUnlockPhase(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		b := a.registerService("s1")
		b.setStatus(pb.Status_ServiceUnLockSucc)
		a.setRestartService("s1")
		if !b.isStatus(pb.Status_ServiceUnLockSucc) {
			t.Fatal("duplicate request regressed ServiceUnLockSucc")
		}
	})
}

func TestDrainLateCleanupCannotRemoveReplacement(t *testing.T) {
	runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
		id := "1234567890123456789uuid1"
		old := a.registerService(id)
		a.disableTableBinds(old)
		current := a.registerService(id)
		old.setStatus(pb.Status_ServiceCanRestart)
		a.disableTableBinds(old)
		if a.getServiceBinds(id) != current || a.canRestartService(id) {
			t.Fatal("late cleanup removed or authorized a replacement bind object")
		}
	})
}

func TestDrainDuplicateRequestEveryPhase(t *testing.T) {
	for _, phase := range []pb.Status{pb.Status_ServiceLockWaiting, pb.Status_ServiceUnLockSucc, pb.Status_ServiceCanRestart} {
		t.Run(phase.String(), func(t *testing.T) {
			runLockTableAllocatorTest(t, time.Hour, func(a *lockTableAllocator) {
				b := a.registerService("s1")
				b.setStatus(phase)
				if !a.setRestartService("s1") || b.getStatus() != phase {
					t.Fatal("duplicate request regressed phase")
				}
			})
		})
	}
}
