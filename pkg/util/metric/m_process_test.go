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
		reg.MustRegister(newBatchStatsCollector(procCpuPercent{}, procMemUsage{}))

		mf, err := reg.Gather()
		So(err, ShouldBeNil)
		So(len(mf), ShouldEqual, 2)
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
		reg.MustRegister(newBatchStatsCollector(procCpuPercent{}, procMemUsage{}, procOpenFds{}, procFdsLimit{}))

		mf, err := reg.Gather()
		So(err, ShouldBeNil)
		So(len(mf), ShouldEqual, 0)
	})
}
