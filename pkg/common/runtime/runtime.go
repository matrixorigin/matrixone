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
	// service -> Runtime
	allRuntime sync.Map
)

type LoggerName int

const (
	Default LoggerName = iota
	SystemInit
)

// GetLogger returns the runtime's logger
func GetLogger(
	sid string,
) *log.MOLogger {
	rt := ServiceRuntime(sid)
	if rt == nil {
		rt = DefaultRuntime()
	}
	return rt.Logger()
}

// SetupServiceBasedRuntime setup service based runtime.
func SetupServiceBasedRuntime(
	service string,
	r Runtime,
) {
	if _, ok := r.GetGlobalVariables(MOProtocolVersion); !ok {
		r.SetGlobalVariables(MOProtocolVersion, defines.MORPCLatestVersion)
	}
	allRuntime.Store(service, r)
}

// ServiceRuntime returns a service based runtime
func ServiceRuntime(
	service string,
) Runtime {
	v, ok := allRuntime.Load(service)
	if !ok {
		if service == "" {
			rt := DefaultRuntime()
			SetupServiceBasedRuntime("", rt)
			return rt
		}
		return nil
	}
	return v.(Runtime)
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
	return DefaultRuntimeWithLevel(zap.InfoLevel)
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

func CheckMethodVersion[Req interface{ GetMethod() T }, T methodType](
	ctx context.Context,
	service string,
	versionMap map[T]int64,
	req Req,
) error {
	return CheckMethodVersionWithRuntime(
		ctx,
		ServiceRuntime(service),
		versionMap,
		req,
	)
}

func CheckMethodVersionWithRuntime[Req interface{ GetMethod() T }, T methodType](
	ctx context.Context,
	rt Runtime,
	versionMap map[T]int64,
	req Req,
) error {
	if version, ok := versionMap[req.GetMethod()]; !ok {
		return moerr.NewNotSupportedf(ctx, "%s not support in current version", req.GetMethod().String())
	} else {
		v, ok := rt.GetGlobalVariables(MOProtocolVersion)
		if !ok {
			return moerr.NewInternalError(ctx, "failed to get protocol version")
		}
		if v.(int64) < version {
			return moerr.NewInternalErrorf(ctx, "unsupported protocol version %d", version)
		}
	}
	return nil
}

// RunTest run runtime test.
func RunTest(
	sid string,
	fn func(rt Runtime),
) {
	v, ok := allRuntime.Load(sid)
	if ok {
		fn(v.(Runtime))
		return
	}

	rt := DefaultRuntime()
	SetupServiceBasedRuntime(sid, rt)
	fn(rt)
}
