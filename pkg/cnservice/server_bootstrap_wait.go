// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cnservice

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// waitForBootstrapUpgrade keeps external SQL and pipeline entry points closed
// until the upgrade owner has committed the final catalog version.
func (s *service) waitForBootstrapUpgrade() error {
	ctx := s.bootstrapUpgradeContext
	if ctx == nil {
		return nil
	}
	result := (<-chan error)(s.bootstrapUpgradeResult)
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	for {
		var err error
		result, err = pollBootstrapUpgradeResult(ctx, result)
		if err != nil {
			return err
		}
		s.bootstrapMu.RLock()
		var complete bool
		if s.bootstrapService != nil {
			complete, err = s.bootstrapService.UpgradeStatus()
		}
		s.bootstrapMu.RUnlock()
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		select {
		case err := <-result:
			result = nil
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return moerr.AttachCause(ctx, ctx.Err())
		case <-timer.C:
		}
	}
}
