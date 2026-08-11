// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resource

// LocalRecorder is owned by exactly one producer. It is intentionally not
// atomic; concurrent work uses independent recorders and publishes once.
type LocalRecorder struct {
	usage Usage
	flags QualityFlags
}

// AddActiveInterval records one producer-local exclusive interval.
func (r *LocalRecorder) AddActiveInterval(wallNS, localWaitNS, childCallNS uint64) {
	active, flags := ExclusiveActive(wallNS, localWaitNS, childCallNS)
	r.usage.ExclusiveActiveNS, r.flags = addChecked(r.usage.ExclusiveActiveNS, active, r.flags|flags)
}

// AddWait records a producer-local wait.
func (r *LocalRecorder) AddWait(kind WaitKind, ns uint64) {
	if kind >= WaitKindCount {
		r.flags |= QualityInvariantFailure
		return
	}
	r.usage.WaitNS[kind], r.flags = addChecked(r.usage.WaitNS[kind], ns, r.flags)
}

// AddS3Request records observed physical object-store operations.
func (r *LocalRecorder) AddS3Request(op S3Op, count uint64) {
	if op >= S3OpCount {
		r.flags |= QualityInvariantFailure
		return
	}
	r.usage.S3Requests[op], r.flags = addChecked(r.usage.S3Requests[op], count, r.flags)
}

// Snapshot returns an immutable copy. The owner must quiesce before calling it.
func (r *LocalRecorder) Snapshot() Delta {
	return Delta{Usage: r.usage, Quality: r.flags}
}
