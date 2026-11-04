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

package client

import (
	"fmt"
	"testing"
)

// Include both ends of a steady admission cycle, not only the dequeue cost.
func BenchmarkActiveTxnQueueCycle(b *testing.B) {
	for _, depth := range []int{1, 8, 1000} {
		b.Run(fmt.Sprint(depth), func(b *testing.B) {
			c := &txnClient{}
			for range depth {
				op := &txnOperator{}
				op.reset.waiter = newActiveTxnWaiter()
				c.mu.waitActiveTxns.pushBack(op)
			}
			b.ReportAllocs()
			for b.Loop() {
				c.mu.Lock()
				claimed := c.claimWaitActiveOpsLocked(1)
				if len(claimed) != 1 {
					b.Fatal("lost admission")
				}
				op := claimed[0]
				op.reset.waiter.mu.state = activeTxnQueued
				c.mu.waitActiveTxns.pushBack(op)
				c.mu.Unlock()
			}
		})
	}
}

// Measure queue ownership work for a whole cohort, including enqueue. Waiter
// construction is outside timing: these benchmarks compare queue mechanics.
func BenchmarkActiveTxnQueueCancelBurst(b *testing.B) {
	for _, depth := range []int{100, 1000, 3000} {
		for _, promote := range []bool{false, true} {
			b.Run(fmt.Sprintf("depth=%d/promote=%t", depth, promote), func(b *testing.B) {
				c := &txnClient{}
				ops := make([]*txnOperator, depth)
				for i := range ops {
					ops[i] = &txnOperator{}
					ops[i].reset.waiter = newActiveTxnWaiter()
					ops[i].reset.waiter.mu.state = activeTxnCanceled
				}
				b.ReportAllocs()
				for b.Loop() {
					c.mu.Lock()
					for _, op := range ops {
						c.mu.waitActiveTxns.pushBack(op)
					}
					start := 0
					if promote {
						ops[0].reset.waiter.mu.state = activeTxnQueued
						if len(c.claimWaitActiveOpsLocked(1)) != 1 {
							b.Fatal("lost head")
						}
						start = 1
					}
					for _, op := range ops[start:] {
						c.mu.waitActiveTxns.remove(op)
					}
					c.mu.Unlock()
					if c.mu.waitActiveTxns.size != 0 {
						b.Fatal("lost cleanup")
					}
				}
			})
		}
	}
}

func BenchmarkActiveTxnWaiterAllocation(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		w := newActiveTxnWaiter()
		w.complete(nil)
	}
}
