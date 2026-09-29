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

import "testing"

func BenchmarkTxnClosureAdmissionRegistration(b *testing.B) {
	service := &service{}
	txnID := []byte("closure-registration")
	b.ReportAllocs()
	for b.Loop() {
		entry := service.refTxnClosureAdmission(txnID)
		service.unrefTxnClosureAdmission(entry)
	}
}

func BenchmarkTxnClosureAdmissionToken(b *testing.B) {
	entry := txnClosureAdmissionPool.Get().(*txnClosureAdmission)
	b.Cleanup(func() { txnClosureAdmissionPool.Put(entry) })
	b.ReportAllocs()
	for b.Loop() {
		select {
		case entry.token <- struct{}{}:
		default:
			b.Fatal("unexpected contended admission")
		}
		<-entry.token
	}
}
