// Copyright 2026 Matrix Origin
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

//go:build sirius && cgo && linux && amd64

package cnservice

import (
	"context"
	"errors"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/siriusbridge"
)

func (s *service) startEmbeddedSiriusRuntime(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c := &s.cfg.Sirius
	if err := validateSiriusEmbeddedConfig(c); err != nil {
		return err
	}
	native, err := siriusbridge.New(siriusbridge.Config{ConfigPath: c.NativeConfigPath, GPUStreams: c.GPUStreams, MaxWaiting: c.MaxWaitingQueries, CleanupTimeout: c.CleanupTimeout.Duration})
	if err != nil {
		return err
	}
	runtime := &compile.SiriusRuntime{EmbeddedMO: true, Backend: &embeddedBackend{native: native}, CleanupTimeout: c.CleanupTimeout.Duration}
	if err = runtime.Validate(); err != nil {
		return errors.Join(err, native.Close(ctx))
	}
	s.siriusRuntime = runtime
	moruntime.ServiceRuntime(s.cfg.UUID).SetGlobalVariables(compile.SiriusRuntimeKey, runtime)
	return nil
}
