// Copyright 2024 Matrix Origin
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

package taskservice

import "sync"

// cronServiceState protect that:
// 1. cannot start when started.
// 2. cannot start when stopping.
// 3. can start after endStop.
// 4. cannot stop when stopping.
// 5. cannot stop after endStop.
// 6. cannot stop when not started.
// 7. can stop when started.
type cronServiceState struct {
	sync.Mutex
	started  bool
	stopping bool
}

func (s *cronServiceState) canStart() bool {
	s.Lock()
	defer s.Unlock()
	if s.stopping || s.started {
		return false
	}
	s.started = true
	return true
}

func (s *cronServiceState) canStop() bool {
	s.Lock()
	defer s.Unlock()
	if !s.started || s.stopping {
		return false
	}
	s.stopping = true
	return true
}

func (s *cronServiceState) endStop() {
	s.Lock()
	defer s.Unlock()
	s.started = false
	s.stopping = false
}

// cronJobState protect that:
// 1. cannot run when updating.
// 2. cannot run when running.
// 3. cannot update when running.
// 4. can update when not running.
// 5. can run when not updating.
type cronJobState struct {
	sync.Mutex
	running  bool
	updating bool
}

func (s *cronJobState) canRun() bool {
	s.Lock()
	defer s.Unlock()
	if s.updating || s.running {
		return false
	}
	s.running = true
	return true
}

func (s *cronJobState) endRun() {
	s.Lock()
	defer s.Unlock()
	s.running = false
	s.updating = false
}

func (s *cronJobState) canUpdate() bool {
	s.Lock()
	defer s.Unlock()
	if s.running {
		return false
	}
	s.updating = true
	return true
}

func (s *cronJobState) endUpdate() {
	s.Lock()
	defer s.Unlock()
	s.running = false
	s.updating = false
}
