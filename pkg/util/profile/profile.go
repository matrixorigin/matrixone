package profile

import (
	"fmt"
	"io"
	"runtime/pprof"
	"time"
)

const (
	GOROUTINE = "goroutine"
	HEAP      = "heap"
	CPU       = "cpu"
)

func ProfileGoroutine(w io.Writer, debug int) error {
	profile := pprof.Lookup("goroutine")
	if err := profile.WriteTo(w, debug); err != nil {
		return err
	}
	return nil
}

func ProfileHeap(w io.Writer, debug int) error {
	profile := pprof.Lookup("heap")
	if profile == nil {
		return nil
	}
	if err := profile.WriteTo(w, debug); err != nil {
		return err
	}
	return nil
}

func ProfileCPU(w io.Writer, d time.Duration) error {
	err := pprof.StartCPUProfile(w)
	if err != nil {
		return err
	}
	time.Sleep(d)
	pprof.StopCPUProfile()
	return nil
}

const timestampFormatter = "20060102_150405.000000"

func Time2DatetimeString(t time.Time) string {
	return t.Format(timestampFormatter)
}

func GetProfileName(typ string, id string, t time.Time) string {
	return fmt.Sprintf("pprof/%s_%s_%s.pprof", typ, id, Time2DatetimeString(t))
}
