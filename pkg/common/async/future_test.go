// Copyright 2021 - 2022 Matrix Origin
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

package async

import (
	"fmt"
	"testing"
	"time"
)

func sleepMs(ms int) (int, error) {
	d, _ := time.ParseDuration(fmt.Sprintf("%dms", ms))
	time.Sleep(d)
	return ms, nil
}

func sleepMany(args ...interface{}) (interface{}, error) {
	var ret int
	for _, ms := range args {
		x, e := sleepMs(ms.(int))
		if e != nil {
			return ret, e
		}
		ret += x
	}
	return ret, nil
}

func TestFuture(t *testing.T) {
	f1 := AsyncCall(sleepMany, 100)
	r1 := f1.MustGet().(int)
	if r1 != 100 {
		t.Errorf("Should slept 100, get %d", r1)
	}

	f2 := AsyncCall(sleepMany, 100, 200, 300)
	r2 := f2.MustGet().(int)
	if r2 != 600 {
		t.Errorf("Should slept 600, get %d", r2)
	}

	fs := make([]*Future, 100)
	var totExp int
	for i := 0; i < 100; i++ {
		i2 := i % 2
		i3 := i % 3
		totExp += i2 + i3
		fs[i] = AsyncCall(sleepMany, i2*100, i3*100)
	}

	var nReady int
	var tot int
	for i := 0; i < 100; i++ {
		if fs[i].IsReady() {
			nReady += 1
		}
	}
	t.Logf("Num ready %d.\n", nReady)

	ms, _ := time.ParseDuration("200ms")
	time.Sleep(ms)
	nReady = 0
	for i := 0; i < 100; i++ {
		if fs[i].IsReady() {
			nReady += 1
		}
	}
	t.Logf("Num ready %d.\n", nReady)

	for i := 0; i < 100; i++ {
		tot += fs[i].MustGet().(int)
	}
	if tot != totExp*100 {
		t.Errorf("Should slept %d, get %d", totExp*100, tot)
	}
	t.Logf("Look, mo, I slept %d*100 ms in no time", totExp)
}
