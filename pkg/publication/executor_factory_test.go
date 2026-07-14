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

package publication

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/require"
)

func TestExecutorFactoryReturnsAttachErrorAndReleasesAdmission(t *testing.T) {
	running.Store(false)
	t.Cleanup(func() { running.Store(false) })

	attachErr := errors.New("attach failed")
	factory := PublicationTaskExecutorFactory(
		nil,
		nil,
		func(context.Context, uint64, taskservice.ActiveRoutine) error {
			return attachErr
		},
		"",
		nil,
		nil,
		nil,
	)

	for range 2 {
		require.ErrorIs(t, factory(context.Background(), &task.DaemonTask{}), attachErr)
	}
}
