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

package fileservice

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

type ctxKeyStatementProfiler struct{}

var CtxKeyStatementProfiler ctxKeyStatementProfiler

var PerStatementProfileDir = os.Getenv("PER_STMT_PROFILE_DIR")

var PerStatementProfileThreshold = func() time.Duration {
	str := os.Getenv("PER_STMT_PROFILE_THRESHOLD_MSEC")
	if str == "" {
		return time.Millisecond * 500
	}
	n, err := strconv.Atoi(str)
	if err != nil {
		panic(err)
	}
	return time.Millisecond * time.Duration(n)
}()

// EnsureStatementProfiler ensure a statement profiler is set in context, if not, copy one from another context
func EnsureStatementProfiler(ctx context.Context, from context.Context) context.Context {
	if v := ctx.Value(CtxKeyStatementProfiler); v != nil {
		// already set
		return ctx
	}
	v := from.Value(CtxKeyStatementProfiler)
	if v == nil {
		// not set in from
		return ctx
	}
	ctx = context.WithValue(ctx, CtxKeyStatementProfiler, v)
	return ctx
}

func NewStatementProfiler(
	ctx context.Context,
) (
	newCtx context.Context,
	end func(
		fileSuffixFunc func() string,
	),
) {
	if PerStatementProfileDir == "" {
		return ctx, nil
	}

	t0 := time.Now()
	profiler := NewSpanProfiler()
	newCtx = context.WithValue(ctx, CtxKeyStatementProfiler, profiler)

	end = func(suffixFunc func() string) {
		duration := time.Since(t0)
		if duration < PerStatementProfileThreshold {
			return
		}

		buf := new(bytes.Buffer)
		if err := profiler.Write(buf); err != nil {
			logutil.Error("fail to write statement profile", zap.Any("error", err))
			return
		}
		if buf.Len() == 0 {
			return
		}

		outputPath := filepath.Join(
			PerStatementProfileDir,
			fmt.Sprintf(
				"%s-%v-%s",
				t0.Format("15-04-05.000000000"),
				duration,
				suffixFunc(),
			),
		)
		err := os.WriteFile(outputPath, buf.Bytes(), 0644)
		if err != nil {
			logutil.Error("fail to write statement profile", zap.Any("file", outputPath), zap.Any("error", err))
			return
		}
		logutil.Info("saved statement profile", zap.Any("file", outputPath))
	}

	return
}

func StatementProfileNewSpan(
	ctx context.Context,
) (
	_ context.Context,
	end func(),
) {
	v := ctx.Value(CtxKeyStatementProfiler)
	if v == nil {
		return ctx, func() {}
	}
	profiler := v.(*SpanProfiler)
	newProfiler, end := profiler.Begin(1)
	if newProfiler != profiler {
		ctx = context.WithValue(ctx, CtxKeyStatementProfiler, newProfiler)
	}
	return ctx, end
}
