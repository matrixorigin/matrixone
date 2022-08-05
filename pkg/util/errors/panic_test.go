package errors

import (
	"context"
	"fmt"
	"testing"
)

func TestRecover(t *testing.T) {
}

func TestRecoverRaw(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			t.Logf("Recover() error = %v", err)
		} else {
			t.Logf("Recover() error = %v, wantErr %v", err, true)
		}
	}()
	panic("TestRecoverRaw")
}

func TestRecoverFunc(t *testing.T) {
	defer Recover(context.Background())
	panic("TestRecoverFunc")
}

func TestReportPanic(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name       string
		args       args
		wantErr    bool
		hasContext bool
		wantMsg    string
	}{
		{
			name:       "normal",
			args:       args{ctx: context.Background()},
			wantErr:    true,
			hasContext: true,
			wantMsg:    "TestReportPanic",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if err := recover(); (err != nil) == tt.wantErr {
					t.Logf("recover() error = %v", err)
					err = ReportPanic(tt.args.ctx, err, 1)
					if fmt.Sprintf("%s", err) != fmt.Sprintf("panic: %s", tt.wantMsg) {
						t.Errorf("ReportPanic() error = %v, wantMsg: %s", err, tt.wantMsg)
					}
					if HasContext(err.(error)) != tt.hasContext {
						t.Errorf("ReportPanic() error = %v, wantErr %v", err, tt.hasContext)
					} else {
						t.Logf("ReportPanic() error = %+v", err)
					}
				} else {
					t.Errorf("Recover() error = %v, wantErr %v", err, tt.wantErr)
				}
			}()
			panic(tt.wantMsg)
		})
	}
}
