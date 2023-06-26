// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metric

import (
	"runtime"
	"testing"

	"github.com/prashantv/gostub"
	prom "github.com/prometheus/client_golang/prometheus"
	. "github.com/smartystreets/goconvey/convey"
)

func TestProcessCollectorNormal(t *testing.T) {
	Convey("collect proc cpu and mem succ", t, func() {
		reg := prom.NewRegistry()
		reg.MustRegister(newBatchStatsCollector(procCpuPercent{}, procMemUsage{}, procCpuTotal{}))

		mf, err := reg.Gather()
		So(err, ShouldBeNil)
		So(len(mf), ShouldEqual, 3)
	})
}

func TestProcessCollectorFD(t *testing.T) {
	Convey("collect fd usage succ", t, func() {
		reg := prom.NewRegistry()
		reg.MustRegister(newBatchStatsCollector(procOpenFds{}, procFdsLimit{}))

		mf, err := reg.Gather()
		So(err, ShouldBeNil)
		if runtime.GOOS == "linux" { // MacOS not implemented yet
			So(len(mf), ShouldEqual, 2)
		}
	})
}

func TestProcessError(t *testing.T) {
	stubs := gostub.StubFunc(&getProcess, nil)
	defer stubs.Reset()
	Convey("collect error", t, func() {
		reg := prom.NewRegistry()
		reg.MustRegister(newBatchStatsCollector(procCpuPercent{}, procMemUsage{}, procOpenFds{}, procFdsLimit{}, procCpuTotal{}))

		mf, err := reg.Gather()
		So(err, ShouldBeNil)
		So(len(mf), ShouldEqual, 0)
	})
}
