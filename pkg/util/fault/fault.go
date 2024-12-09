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

// A very simple fault injection tool.
package fault

import (
	"context"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	STOP = iota
	LOOKUP
	ADD
	REMOVE
	TRIGGER
	LIST
)

const (
	RETURN = iota
	GETCOUNT
	SLEEP
	WAIT
	GETWAITERS
	NOTIFY
	NOTIFYALL
	PANIC
	ECHO
)

const (
	// PANIC with non-moerr
	PanicUseNonMoErr = 0
	// PANIC with moerr.NewXXXErr
	PanicUseMoErr = 1
)

// faultEntry describes how we shall fail
type faultEntry struct {
	cmd              int     // command
	name             string  // name of the fault
	cnt              int     // count how many times we run into this
	start, end, skip int     // start, end, skip
	prob             float64 // probability of failure
	action           int
	iarg             int64  // int arg
	sarg             string // string arg
	constant         bool

	nWaiters int
	mutex    sync.Mutex
	cond     *sync.Cond
}

type faultMap struct {
	faultPoints map[string]*faultEntry
	chIn        chan *faultEntry
	chOut       chan *faultEntry
}

var enabled atomic.Pointer[faultMap]

func (fm *faultMap) run() {
	for {
		e := <-fm.chIn
		switch e.cmd {
		case STOP:
			return
		case ADD:
			if v, ok := fm.faultPoints[e.name]; ok && (v.constant || e.constant) {
				fm.chOut <- nil
			} else {
				fm.faultPoints[e.name] = e
				fm.chOut <- e
			}
		case REMOVE:
			if e.name == "all" {
				fm.faultPoints = make(map[string]*faultEntry)
				fm.chOut <- e
				continue
			}
			if v, ok := fm.faultPoints[e.name]; ok {
				delete(fm.faultPoints, e.name)
				fm.chOut <- v
			} else {
				fm.chOut <- nil
			}
		case TRIGGER:
			var out *faultEntry
			if v, ok := fm.faultPoints[e.name]; ok {
				v.cnt += 1
				if v.cnt >= v.start && v.cnt <= v.end && (v.cnt-v.start)%v.skip == 0 {
					if v.prob == 1 || rand.Float64() < v.prob {
						out = v
					}
				}
			}
			fm.chOut <- out
		case LOOKUP:
			fm.chOut <- fm.faultPoints[e.sarg]
		case LIST:
			for _, v := range fm.faultPoints {
				fm.chOut <- v
			}
			fm.chOut <- nil
		default:
			fm.chOut <- nil
		}
	}
}

func (e *faultEntry) do() (int64, string) {
	switch e.action {
	case RETURN: // no op
	case SLEEP:
		time.Sleep(time.Duration(e.iarg) * time.Second)
	case GETCOUNT:
		if ee := lookup(e.sarg); ee != nil {
			return int64(ee.cnt), ""
		}
	case WAIT:
		e.mutex.Lock()
		e.nWaiters += 1
		e.cond.Wait()
		e.nWaiters -= 1
		e.mutex.Unlock()
	case GETWAITERS:
		if ee := lookup(e.sarg); ee != nil {
			ee.mutex.Lock()
			nw := ee.nWaiters
			ee.mutex.Unlock()
			return int64(nw), ""
		}
	case NOTIFY:
		if ee := lookup(e.sarg); ee != nil {
			ee.cond.Signal()
		}
	case NOTIFYALL:
		if ee := lookup(e.sarg); ee != nil {
			ee.cond.Broadcast()
		}
	case PANIC:
		switch e.iarg {
		case PanicUseMoErr:
			panic(moerr.NewInternalError(context.Background(), e.sarg))
		default:
			panic(e.sarg)
		}
	case ECHO:
		return e.iarg, e.sarg
	}
	return 0, ""
}

func startFaultMap() bool {
	if enabled.Load() != nil {
		return false
	}
	fm := new(faultMap)
	fm.faultPoints = make(map[string]*faultEntry)
	fm.chIn = make(chan *faultEntry)
	fm.chOut = make(chan *faultEntry)
	go fm.run()
	if !enabled.CompareAndSwap(nil, fm) {
		var msg faultEntry
		msg.cmd = STOP
		fm.chIn <- &msg
		return false
	}
	return true
}

func stopFaultMap() bool {
	fm := enabled.Load()
	if fm == nil {
		return false
	}
	if !enabled.CompareAndSwap(fm, nil) {
		return false
	}

	var msg faultEntry
	msg.cmd = STOP
	fm.chIn <- &msg
	return true
}

// Enable fault injection
func Enable() bool {
	return startFaultMap()
}

// Disable fault injection
func Disable() bool {
	return stopFaultMap()
}

func Status() bool {
	return enabled.Load() != nil
}

// Trigger a fault point.
func TriggerFault(name string) (iret int64, sret string, exist bool) {
	fm := enabled.Load()
	if fm == nil {
		return
	}
	var msg faultEntry
	msg.cmd = TRIGGER
	msg.name = name
	fm.chIn <- &msg
	out := <-fm.chOut

	if out == nil {
		return
	}
	exist = true
	iret, sret = out.do()
	return
}

func AddFaultPoint(ctx context.Context, name string, freq string, action string, iarg int64, sarg string, constant bool) error {
	fm := enabled.Load()
	if fm == nil {
		return moerr.NewInternalError(ctx, "add fault point not enabled")
	}

	var err error

	// Build msg from input.
	var msg faultEntry
	msg.cmd = ADD
	msg.name = name

	// freq is start:end:skip:prob
	sesp := strings.Split(freq, ":")
	if len(sesp) != 4 {
		return moerr.NewInvalidArg(ctx, "fault point freq", freq)
	}

	if sesp[0] == "" {
		msg.start = 1
	} else {
		msg.start, err = strconv.Atoi(sesp[0])
		if err != nil {
			return moerr.NewInvalidArg(ctx, "fault point freq", freq)
		}
	}
	if sesp[1] == "" {
		msg.end = math.MaxInt
	} else {
		msg.end, err = strconv.Atoi(sesp[1])
		if err != nil || msg.end < msg.start {
			return moerr.NewInvalidArg(ctx, "fault point freq", freq)
		}
	}
	if sesp[2] == "" {
		msg.skip = 1
	} else {
		msg.skip, err = strconv.Atoi(sesp[2])
		if err != nil || msg.skip <= 0 {
			return moerr.NewInvalidArg(ctx, "fault point freq", freq)
		}
	}
	if sesp[3] == "" {
		msg.prob = 1.0
	} else {
		msg.prob, err = strconv.ParseFloat(sesp[3], 64)
		if err != nil || msg.prob <= 0 || msg.prob >= 1 {
			return moerr.NewInvalidArg(ctx, "fault point freq", freq)
		}
	}

	// Action
	switch strings.ToUpper(action) {
	case "RETURN":
		msg.action = RETURN
	case "SLEEP":
		msg.action = SLEEP
	case "GETCOUNT":
		msg.action = GETCOUNT
	case "WAIT":
		msg.action = WAIT
	case "GETWAITERS":
		msg.action = GETWAITERS
	case "NOTIFY":
		msg.action = NOTIFY
	case "NOTIFYALL":
		msg.action = NOTIFYALL
	case "PANIC":
		msg.action = PANIC
	case "ECHO":
		msg.action = ECHO
	default:
		return moerr.NewInvalidArg(ctx, "fault action", action)
	}

	msg.iarg = iarg
	msg.sarg = sarg
	msg.constant = constant

	if msg.action == WAIT {
		msg.cond = sync.NewCond(&msg.mutex)
	}

	fm.chIn <- &msg
	out := <-fm.chOut
	if out == nil {
		return moerr.NewInternalError(
			ctx,
			"failed to add fault point; it may already exist and be constant.",
		)
	}
	return nil
}

func RemoveFaultPoint(ctx context.Context, name string) (bool, error) {
	fm := enabled.Load()
	if fm == nil {
		return false, moerr.NewInternalError(ctx, "fault injection not enabled.")
	}

	var msg faultEntry
	msg.cmd = REMOVE
	msg.name = name
	fm.chIn <- &msg
	out := <-fm.chOut
	if out == nil {
		return false, nil
	}
	return true, nil
}

func lookup(name string) *faultEntry {
	fm := enabled.Load()
	if fm == nil {
		return nil
	}

	var msg faultEntry
	msg.cmd = LOOKUP
	msg.sarg = name
	fm.chIn <- &msg
	out := <-fm.chOut
	return out
}

type Point struct {
	Name string `json:"name"`
	Iarg int64  `json:"iarg"`
	Sarg string `json:"sarg"`

	Constant bool `json:"constant"`
}

func ListAllFaultPoints() string {
	fm := enabled.Load()
	if fm == nil {
		return "list fault points not enabled"
	}

	points := make([]Point, 0)

	var msg faultEntry
	msg.cmd = LIST
	fm.chIn <- &msg
	for {
		out := <-fm.chOut
		if out == nil {
			break
		}
		points = append(points, Point{
			Name:     out.name,
			Iarg:     out.iarg,
			Sarg:     out.sarg,
			Constant: out.constant,
		})
	}

	data, _ := jsoniter.Marshal(points)

	return string(data)
}
