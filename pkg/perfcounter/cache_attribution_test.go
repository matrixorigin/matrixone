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

package perfcounter

import (
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheAttributionReportingAndReset(t *testing.T) {
	var counters CounterSet
	var visit func(reflect.Value, func(*atomic.Int64))
	visit = func(value reflect.Value, fn func(*atomic.Int64)) {
		if value.Type() == reflect.TypeFor[atomic.Int64]() {
			fn(value.Addr().Interface().(*atomic.Int64))
			return
		}
		if value.Kind() == reflect.Struct {
			for i := 0; i < value.NumField(); i++ {
				visit(value.Field(i), fn)
			}
		}
	}
	visit(reflect.ValueOf(&counters).Elem(), func(c *atomic.Int64) { c.Store(3) })
	output := httptest.NewRecorder()
	counters.ServeHTTP(output, httptest.NewRequest("GET", "/", nil))
	for _, name := range []string{"ReaderCacheWriteBypassBlocks", "ColumnConstruction", "LZ4", "UpdatePolicySkips", "UpdatePressureSkips"} {
		require.Contains(t, output.Body.String(), name)
	}
	counters.Reset()
	visit(reflect.ValueOf(&counters).Elem(), func(c *atomic.Int64) { require.Zero(t, c.Load()) })
}

func TestCacheAttributionConcurrentReporting(t *testing.T) {
	var counters CounterSet
	stop, ready, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		counters.FileService.ColumnConstruction.LZ4.Calls.Add(1)
		close(ready)
		for {
			select {
			case <-stop:
				return
			default:
				counters.FileService.ColumnConstruction.LZ4.Calls.Add(1)
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-done })
	<-ready
	for i := 0; i < 20; i++ {
		output := httptest.NewRecorder()
		counters.ServeHTTP(output, httptest.NewRequest("GET", "/", nil))
		require.Contains(t, output.Body.String(), "LZ4")
	}
}
