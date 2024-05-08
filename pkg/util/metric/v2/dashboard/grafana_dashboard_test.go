// Copyright 2023 Matrix Origin
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

package dashboard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCreateCloudDashboard creates a dashboard for cloud env.
func TestCreateCloudDashboard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	c := NewCloudDashboardCreator("http://127.0.0.1", "admin", "admin", "Prometheus")
	require.NoError(t, c.Create())
}

// TestCreateLocalDashboard creates a dashboard for local env.
func TestCreateLocalDashboard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	c := NewLocalDashboardCreator("http://127.0.0.1", "admin", "admin", "Prometheus")
	require.NoError(t, c.Create())
}

// TestCreateK8SDashboard creates a dashboard for k8s env(EKS, TKE).
func TestCreateK8SDashboard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	c := NewK8SDashboardCreator("http://127.0.0.1", "admin", "admin", "Prometheus")
	require.NoError(t, c.Create())
}

// TestCreateCloudCtrlPlaneDashboard creates a dashboard for cloud env. (used in control-plane)
// diff TestCreateCloudDashboard, which is used in data-plane (unit).
func TestCreateCloudCtrlPlaneDashboard(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	c := NewCloudCtrlPlaneDashboardCreator("http://127.0.0.1", "admin", "admin", "Prometheus")
	require.NoError(t, c.Create())
}
