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

package cnservice

import (
	"context"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
)

func (s *service) startSiriusRuntime(ctx context.Context) error {
	if !s.cfg.Sirius.Enabled {
		return nil
	}
	return s.startEmbeddedSiriusRuntime(ctx)
}
func (s *service) closeSiriusRuntime() error {
	if s.siriusRuntime == nil {
		return nil
	}
	runtime := s.siriusRuntime
	timeout := s.cfg.Sirius.CleanupTimeout.Duration
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	s.siriusRuntime = nil
	moruntime.ServiceRuntime(s.cfg.UUID).CompareAndDeleteGlobalVariables(compile.SiriusRuntimeKey, runtime)
	return nil
}
