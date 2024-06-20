// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Portions of this file are additionally subject to the following
// copyright.
//
// Copyright (C) 2021 Matrix Origin.
//
// Modified the behavior of the status tracker.

package operator

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSTARTED(t *testing.T) {
	before := time.Now()
	trk := NewOpStatusTracker()
	assert.Equal(t, STARTED, trk.Status())
	assert.True(t, reflect.DeepEqual(trk.ReachTimeOf(STARTED), trk.ReachTime()))
	checkTimeOrder(t, before, trk.ReachTime(), time.Now())
	checkReachTime(t, &trk, STARTED)
}

func TestEndStatusTrans(t *testing.T) {
	allStatus := make([]OpStatus, 0, statusCount)
	for st := OpStatus(0); st < statusCount; st++ {
		allStatus = append(allStatus, st)
	}
	for from := firstEndStatus; from < statusCount; from++ {
		trk := NewOpStatusTracker()
		trk.current = from
		assert.True(t, trk.IsEnd())
		checkInvalidTrans(t, &trk, allStatus...)
	}
}

func TestStatusCheckExpired(t *testing.T) {
	// Not expired
	{
		before := time.Now()
		time.Sleep(time.Millisecond * 10)
		trk := NewOpStatusTracker()
		time.Sleep(time.Millisecond * 10)
		after := time.Now()
		assert.False(t, trk.CheckExpired(10*time.Second))
		assert.Equal(t, STARTED, trk.Status())
		checkTimeOrder(t, before, trk.ReachTime(), after)
	}
	{
		// Expired but status not changed
		trk := NewOpStatusTracker()
		trk.setTime(STARTED, time.Now().Add(-10*time.Second))
		assert.True(t, trk.CheckExpired(5*time.Second))
		assert.Equal(t, EXPIRED, trk.Status())
	}
	{
		// Expired and status changed
		trk := NewOpStatusTracker()
		before := time.Now()
		assert.True(t, trk.To(EXPIRED))
		after := time.Now()
		assert.True(t, trk.CheckExpired(0))
		assert.Equal(t, EXPIRED, trk.Status())
		checkTimeOrder(t, before, trk.ReachTime(), after)
	}
}

func checkTimeOrder(t *testing.T, t1, t2, t3 time.Time) {
	assert.True(t, t1.Before(t2))
	assert.True(t, t3.After(t2))
}

func checkInvalidTrans(t *testing.T, trk *OpStatusTracker, sts ...OpStatus) {
	origin := trk.Status()
	originTime := trk.ReachTime()
	sts = append(sts, statusCount, statusCount+1, statusCount+10)
	for _, st := range sts {
		assert.False(t, trk.To(st))
		assert.Equal(t, origin, trk.Status())
		assert.True(t, reflect.DeepEqual(originTime, trk.ReachTime()))
	}
}

func checkReachTime(t *testing.T, trk *OpStatusTracker, reached ...OpStatus) {
	reachedMap := make(map[OpStatus]struct{}, len(reached))
	for _, st := range reached {
		assert.False(t, trk.ReachTimeOf(st).IsZero())
		reachedMap[st] = struct{}{}
	}
	for st := OpStatus(0); st <= statusCount+10; st++ {
		if _, ok := reachedMap[st]; ok {
			continue
		}
		assert.True(t, trk.ReachTimeOf(st).IsZero())
	}
}
