// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSpanProfiler(t *testing.T) {
	profiler := NewSpanProfiler()

	newProfiler, end := profiler.Begin(0)
	assert.True(t, newProfiler == profiler)

	done := make(chan struct{})
	go func() {
		_, end := newProfiler.Begin(0)
		time.Sleep(time.Millisecond * 500)
		end()
		close(done)
	}()

	time.Sleep(time.Millisecond * 800)
	<-done
	end()
	err := profiler.Write(io.Discard)
	assert.Nil(t, err)

	//f, err := os.Create("prof")
	//assert.Nil(t, err)
	//defer f.Close()
	//err = profiler.Write(f)
	//assert.Nil(t, err)
}
