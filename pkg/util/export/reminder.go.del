package export

import (
	"time"
)

type NormalReminder struct {
	interval time.Duration
}

func NewNormalReminder(interval time.Duration) *NormalReminder {
	return &NormalReminder{interval: interval}
}

func (r *NormalReminder) RemindNextAfter() time.Duration {
	return r.interval
}

func (r *NormalReminder) RemindBackOffCnt() int {
	return 0
}

func (r *NormalReminder) RemindBackOff() {
}

func (r *NormalReminder) RemindReset() {}
