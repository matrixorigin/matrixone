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

package compile

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/stretchr/testify/require"
)

func TestPipelineStreamLifecycleTimeoutRemovesRegistration(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	lifecycle, err := registerPipelineStreamLifecycle(session, 101)
	require.NoError(t, err)
	oldTimeout := pipelineStreamFinishTimeout
	pipelineStreamFinishTimeout = 10 * time.Millisecond
	t.Cleanup(func() {
		pipelineStreamFinishTimeout = oldTimeout
		lifecycle.remove()
	})

	require.False(t, lifecycle.waitForFinish(context.Background(), context.Background()))
	_, exists := pipelineStreamLifecycles.Load(lifecycle.key)
	require.False(t, exists)
}

func TestPipelineStreamLifecycleDuplicatePoisonsSession(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	session.EXPECT().Close().Return(nil)
	first, err := registerPipelineStreamLifecycle(session, 102)
	require.NoError(t, err)
	defer first.remove()
	_, err = registerPipelineStreamLifecycle(session, 102)
	require.Error(t, err)
}

func TestPipelineStreamFinishWithoutValidatedTokenPoisonsSession(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	session.EXPECT().Close().Return(nil)
	err := handlePipelineStreamFinish(
		context.Background(),
		&pipeline.Message{Id: 103, Cmd: pipeline.Method_PipelineStreamFinish, Sid: pipeline.Status_Last},
		session,
		func() morpc.Message { return &pipeline.Message{} },
	)
	require.Error(t, err)
}
