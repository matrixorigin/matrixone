// Copyright 2024 Matrix Origin
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

package compile

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func Test_handlePipelineMessage(t *testing.T) {
	_ = colexec.NewServer(nil)
	srv := colexec.Get()

	proc := testutil.NewProcess(t)

	withTimeoutCauseAdapter := func(parent context.Context, timeout time.Duration, cause error) (context.Context, context.CancelCauseFunc) {
		ctx, cancel := context.WithTimeout(parent, timeout)
		return ctx, func(c error) {
			cancel()
			if c != nil {
				cause = c
			}
		}
	}
	proc.Ctx, proc.Cancel = withTimeoutCauseAdapter(context.Background(), time.Second*0, moerr.NewInternalErrorNoCtx("ut tester"))
	_ = srv.PutProcIntoUuidMap(uuid.UUID{}, proc, nil)

	recv := &messageReceiverOnServer{}
	recv.messageTyp = pipeline.Method_PrepareDoneNotifyMessage
	recv.connectionCtx = context.Background()
	_ = handlePipelineMessage(recv)
}
