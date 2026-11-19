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

package stopper

import (
	"sync/atomic"
	"testing"
)

func TestAdmissionGateSealWaitsForSenders(t *testing.T) {
	var gate AdmissionGate
	var closed atomic.Bool
	if !gate.Enter(&closed) {
		t.Fatal("open gate rejected a sender")
	}
	closed.Store(true)
	canceled := make(chan struct{})
	sealed := make(chan struct{})
	go func() {
		gate.Seal(func() { close(canceled) })
		close(sealed)
	}()
	<-canceled
	if gate.Enter(&closed) {
		t.Fatal("closed gate accepted a sender")
	}
	select {
	case <-sealed:
		t.Fatal("seal returned before the sender left")
	default:
	}
	gate.Leave()
	<-sealed
}
