package util

import (
	"testing"
	"time"
)

var timeFormat = "2006-01-02 15:04:05"
var timeUSFormat = "2006-01-02 15:04:05.000000"

func TestNow(t *testing.T) {
	tests := []struct {
		name  string
		delta time.Duration
		want  time.Time
	}{
		{
			name:  "normal",
			delta: 100 * time.Nanosecond,
			want:  time.Now(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.want = tt.want.Add(tt.delta)
			if got := Now(); got.Second() != tt.want.Second() || got.Format(timeFormat) != tt.want.Format(timeFormat) {
				t.Errorf("Now() = %v, want %v", got, tt.want)
			} else {
				t.Logf("Now() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNowNS(t *testing.T) {
	tests := []struct {
		name string
		want TimeNano
	}{
		{
			name: "normal",
			want: NowNS(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NowNS(); got <= tt.want {
				t.Errorf("NowNS() = %v, want %v", got, tt.want)
			}
		})
	}
}
