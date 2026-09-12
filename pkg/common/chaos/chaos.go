// Copyright 2021 Matrix Origin
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

package chaos

import (
	"database/sql"
	"sync"
	"time"
)

// ChaosTester chaos tester
type ChaosTester struct {
	mu        sync.Mutex
	testers   []tester
	started   bool
	stopped   bool
	stopC     chan struct{}
	startOnce sync.Once
	startDone chan struct{}
}

// NewChaosTester create chaos tester
func NewChaosTester(cfg Config) *ChaosTester {
	t := &ChaosTester{stopC: make(chan struct{}), startDone: make(chan struct{})}
	t.testers = append(t.testers, newRestartTester(cfg))
	return t
}

func (t *ChaosTester) waitSystemBootStrapCompleted() bool {
	for {
		ok, err := t.doCheckBootStrapCompleted()
		if err != nil || !ok {
			select {
			case <-t.stopC:
				return false
			case <-time.After(time.Second * 5):
			}
			continue
		}
		return true
	}
}

func (t *ChaosTester) doCheckBootStrapCompleted() (bool, error) {
	db, err := sql.Open("mysql", "dump:111@tcp(127.0.0.1:6001)/mo_catalog")
	defer func() {
		_ = db.Close()
	}()
	if err != nil {
		return false, err
	}

	res, err := db.Query("show tables")
	if err != nil {
		return false, err
	}
	defer func() {
		_ = res.Close()
	}()
	if res.Err() != nil {
		return false, res.Err()
	}

	var name string
	for res.Next() {
		if err := res.Scan(&name); err != nil {
			return false, err
		}
		if name == "mo_account" {
			return true, nil
		}
	}
	return false, nil
}

func (t *ChaosTester) Start() error {
	return t.startWith(t.waitSystemBootStrapCompleted)
}

func (t *ChaosTester) startWith(wait func() bool) error {
	t.startOnce.Do(func() {
		go func() {
			defer close(t.startDone)
			if !wait() {
				return
			}
			t.mu.Lock()
			defer t.mu.Unlock()
			if t.stopped {
				return
			}
			for _, tester := range t.testers {
				if err := tester.start(); err != nil {
					panic(err)
				}
			}
			t.started = true
		}()
	})
	return nil
}

func (t *ChaosTester) Stop() error {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return nil
	}
	t.stopped = true
	close(t.stopC)
	if !t.started {
		t.mu.Unlock()
		return nil
	}
	testers := append([]tester(nil), t.testers...)
	t.mu.Unlock()
	for _, tester := range testers {
		if err := tester.stop(); err != nil {
			return err
		}
	}
	return nil
}
