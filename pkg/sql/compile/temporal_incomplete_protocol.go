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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Check both initial compilation and cached execution before operator Prepare.
// A cached plan cannot retain permission after the negotiated floor rolls back.
func validateIncompleteTemporalStringProtocol(proc *process.Process, owner any) error {
	if proc != nil {
		if version, known := remoteMORPCProtocolVersion(proc.GetService()); known && version >= defines.MORPCVersion126 {
			return nil
		}
	}
	features, err := plan.RequiredRemoteExpressionFeatures(owner)
	if err != nil {
		return err
	}
	if features.IncompleteTemporalStrings {
		return moerr.NewNotSupportedNoCtx("incomplete temporal strings require MORPC protocol version 126")
	}
	return nil
}
