package util

import (
	"fmt"
	"github.com/pkg/errors"
	"reflect"
	"testing"
)

func TestCaller(t *testing.T) {
	type args struct {
		depth int
	}
	tests := []struct {
		name string
		args args
		want Frame
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Caller(tt.args.depth); got != tt.want {
				t.Errorf("Caller() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCallers(t *testing.T) {
	type args struct {
		depth int
	}
	tests := []struct {
		name string
		args args
		want *Stack
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Callers(tt.args.depth); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Callers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStack_Format(t *testing.T) {
	type args struct {
		st   fmt.State
		verb rune
	}
	tests := []struct {
		name string
		s    Stack
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.Format(tt.args.st, tt.args.verb)
		})
	}
}

func TestStack_StackTrace(t *testing.T) {
	tests := []struct {
		name string
		s    Stack
		want errors.StackTrace
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.StackTrace(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("StackTrace() = %v, want %v", got, tt.want)
			}
		})
	}
}
