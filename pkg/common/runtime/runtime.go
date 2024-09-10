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

package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	processLevel atomic.Value
)

type LoggerName int

const (
	Default LoggerName = iota
	SystemInit
)

// ProcessLevelRuntime returns a process-lelve runtime
func ProcessLevelRuntime() Runtime {
	v := processLevel.Load()
	if v == nil {
		return nil
	}
	return v.(Runtime)
}

// SetupProcessLevelRuntime set a process-level runtime. If the service does not
// support a service-level runtime when running in launch mode, it will use the
// process-level runtime. The process-level runtime must setup in main.
func SetupProcessLevelRuntime(r Runtime) {
	processLevel.Store(r)
	if _, ok := ProcessLevelRuntime().GetGlobalVariables(MOProtocolVersion); !ok {
		ProcessLevelRuntime().SetGlobalVariables(MOProtocolVersion, defines.MORPCLatestVersion)
	}
}

// WithClock setup clock for a runtime, CN and TN must contain an instance of the
// Clock that is used to provide the timestamp service to the transaction.
func WithClock(clock clock.Clock) Option {
	return func(r *runtime) {
		r.global.clock = clock
	}
}

// NewRuntime create a mo runtime environment.
func NewRuntime(service metadata.ServiceType, uuid string, logger *zap.Logger, opts ...Option) Runtime {
	rt := &runtime{
		serviceType: service,
		serviceUUID: uuid,
	}
	for _, opt := range opts {
		opt(rt)
	}
	rt.global.logger = log.GetServiceLogger(logutil.Adjust(logger), service, uuid)
	rt.initSystemInitLogger()
	return rt
}

type runtime struct {
	serviceType metadata.ServiceType
	serviceUUID string

	global struct {
		clock     clock.Clock
		logger    *log.MOLogger
		variables sync.Map

		systemInitLogger *log.MOLogger
	}
}

func (r *runtime) Logger() *log.MOLogger {
	return r.global.logger
}

func (r *runtime) SubLogger(name LoggerName) *log.MOLogger {
	switch name {
	case SystemInit:
		return r.global.systemInitLogger
	default:
		return r.Logger()
	}
}

func (r *runtime) Clock() clock.Clock {
	return r.global.clock
}

func (r *runtime) ServiceType() metadata.ServiceType {
	return r.serviceType
}

func (r *runtime) ServiceUUID() string {
	return r.serviceUUID
}

func (r *runtime) SetGlobalVariables(name string, value any) {
	r.global.variables.Store(name, value)
}

func (r *runtime) GetGlobalVariables(name string) (any, bool) {
	return r.global.variables.Load(name)
}

// DefaultRuntime used to test
func DefaultRuntime() Runtime {
	return DefaultRuntimeWithLevel(zap.DebugLevel)
}

// DefaultRuntime used to test
func DefaultRuntimeWithLevel(level zapcore.Level) Runtime {
	return NewRuntime(
		metadata.ServiceType_CN,
		"",
		logutil.GetPanicLoggerWithLevel(level),
		WithClock(clock.NewHLCClock(func() int64 {
			return time.Now().UTC().UnixNano()
		}, 0)))
}

func (r *runtime) initSystemInitLogger() {
	if r.global.logger == nil {
		r.global.logger = log.GetServiceLogger(logutil.Adjust(nil), r.serviceType, r.serviceUUID)
	}
	r.global.systemInitLogger = r.Logger().WithProcess(log.SystemInit)
}

type methodType interface {
	~int32
	String() string
}

func CheckMethodVersion[Req interface{ GetMethod() T }, T methodType](ctx context.Context, versionMap map[T]int64, req Req) error {
	if version, ok := versionMap[req.GetMethod()]; !ok {
		return moerr.NewNotSupportedf(ctx, "%s not support in current version", req.GetMethod().String())
	} else {
		v, ok := ProcessLevelRuntime().GetGlobalVariables(MOProtocolVersion)
		if !ok {
			return moerr.NewInternalError(ctx, "failed to get protocol version")
		}
		if v.(int64) < version {
			return moerr.NewInternalErrorf(ctx, "unsupported protocol version %d", version)
		}
	}
	return nil
}
