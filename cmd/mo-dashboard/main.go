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

package mo_dashboard

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/util/metric/v2/dashboard"
	"github.com/spf13/cobra"
)

func PrepareCommand() *cobra.Command {
	var (
		host       string
		port       int
		mode       string
		username   string
		password   string
		datasource string
	)

	cmd := &cobra.Command{
		Use:   "dashboard",
		Short: "Create Grafana dashboards for MatrixOne",
		Long: `Create Grafana dashboards for MatrixOne monitoring.

This command connects to a Grafana instance and creates all MatrixOne dashboards.
Supports different modes: local (standalone), cloud (docker compose cluster), k8s, and cloud-ctrl.`,
		Example: `  # Create local dashboard (default: port 3001)
  mo-tool dashboard

  # Create dashboard for docker compose cluster (port 3000)
  mo-tool dashboard --mode cloud --port 3000

  # Create dashboard with custom host and credentials
  mo-tool dashboard --host localhost --port 3001 --username admin --password mypass`,
		RunE: func(cmd *cobra.Command, args []string) error {
			grafanaURL := fmt.Sprintf("http://%s:%d", host, port)

			var creator *dashboard.DashboardCreator

			switch mode {
			case "local":
				creator = dashboard.NewLocalDashboardCreator(
					grafanaURL,
					username,
					password,
					"Matrixone-Standalone",
				)
				fmt.Printf("Creating local dashboard at %s (folder: Matrixone-Standalone)\n", grafanaURL)

			case "cloud":
				creator = dashboard.NewCloudDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				fmt.Printf("Creating cloud dashboard at %s (datasource: %s, folder: Matrixone)\n", grafanaURL, datasource)

			case "k8s":
				creator = dashboard.NewK8SDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				fmt.Printf("Creating K8S dashboard at %s (datasource: %s, folder: Matrixone)\n", grafanaURL, datasource)

			case "cloud-ctrl":
				creator = dashboard.NewCloudCtrlPlaneDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				fmt.Printf("Creating cloud control-plane dashboard at %s (datasource: %s, folder: Matrixone)\n", grafanaURL, datasource)

			default:
				return fmt.Errorf("invalid mode '%s'. Valid modes: local, cloud, k8s, cloud-ctrl", mode)
			}

			if err := creator.Create(); err != nil {
				return fmt.Errorf("failed to create dashboard: %w", err)
			}

			fmt.Println("✅ Dashboard created successfully!")
			return nil
		},
	}

	cmd.Flags().StringVar(&host, "host", "127.0.0.1", "Grafana host address")
	cmd.Flags().IntVar(&port, "port", 3001, "Grafana port (default: 3001 for local, 3000 for docker compose)")
	cmd.Flags().StringVar(&mode, "mode", "local", "Dashboard mode: local, cloud, k8s, cloud-ctrl (default: local)")
	cmd.Flags().StringVar(&username, "username", "admin", "Grafana username (default: admin)")
	cmd.Flags().StringVar(&password, "password", "admin", "Grafana password (default: admin)")
	cmd.Flags().StringVar(&datasource, "datasource", "Prometheus", "Prometheus datasource name (for cloud/k8s modes, default: Prometheus)")

	return cmd
}
