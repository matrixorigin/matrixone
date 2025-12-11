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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
		Short: "Manage Grafana dashboards for MatrixOne",
		Long: `Manage Grafana dashboards for MatrixOne monitoring.

This command connects to a Grafana instance and manages MatrixOne dashboards.
Supports create, list, delete operations for dashboards and folders.
Supports different modes: local (standalone), cloud (docker compose cluster), k8s, and cloud-ctrl.

Available operations:
  - create: Create all MatrixOne dashboards
  - list: List all dashboards in the folder
  - delete: Delete all dashboards in the folder
  - delete-dashboard: Delete a single dashboard by UID
  - delete-folder: Delete the entire folder and all dashboards in it

Note: Provisioned dashboards (created via config files) cannot be deleted via API
and will be skipped with a warning when deleting folders.`,
	}

	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create Grafana dashboards for MatrixOne",
		Long: `Create Grafana dashboards for MatrixOne monitoring.

This command connects to a Grafana instance and creates all MatrixOne dashboards.
Supports different modes: local (standalone), cloud (docker compose cluster), k8s, and cloud-ctrl.`,
		Example: `  # Create local dashboard (default: port 3001)
  mo-tool dashboard create

  # Create dashboard for docker compose cluster (port 3000)
  mo-tool dashboard create --mode cloud --port 3000

  # Create dashboard with custom host and credentials
  mo-tool dashboard create --host localhost --port 3001 --username admin --password mypass`,
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
				logutil.Infof("Creating local dashboard at %s (folder: Matrixone-Standalone)", grafanaURL)

			case "cloud":
				creator = dashboard.NewCloudDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				logutil.Infof("Creating cloud dashboard at %s (datasource: %s, folder: Matrixone)", grafanaURL, datasource)

			case "k8s":
				creator = dashboard.NewK8SDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				logutil.Infof("Creating K8S dashboard at %s (datasource: %s, folder: Matrixone)", grafanaURL, datasource)

			case "cloud-ctrl":
				creator = dashboard.NewCloudCtrlPlaneDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				logutil.Infof("Creating cloud control-plane dashboard at %s (datasource: %s, folder: Matrixone)", grafanaURL, datasource)

			default:
				return fmt.Errorf("invalid mode '%s'. Valid modes: local, cloud, k8s, cloud-ctrl", mode)
			}

			if err := creator.Create(); err != nil {
				return fmt.Errorf("failed to create dashboard: %w", err)
			}

			logutil.Infof("Dashboard created successfully!")
			return nil
		},
	}

	deleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete Grafana dashboards for MatrixOne",
		Long: `Delete Grafana dashboards for MatrixOne monitoring.

This command connects to a Grafana instance and deletes all MatrixOne dashboards in the specified folder.
Supports different modes: local (standalone), cloud (docker compose cluster), k8s, and cloud-ctrl.`,
		Example: `  # Delete local dashboards (default: port 3001)
  mo-tool dashboard delete

  # Delete dashboards for docker compose cluster (port 3000)
  mo-tool dashboard delete --mode cloud --port 3000

  # Delete dashboards with custom host and credentials
  mo-tool dashboard delete --host localhost --port 3001 --username admin --password mypass`,
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
				logutil.Infof("Deleting local dashboards at %s (folder: Matrixone-Standalone)", grafanaURL)

			case "cloud":
				creator = dashboard.NewCloudDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				logutil.Infof("Deleting cloud dashboards at %s (folder: Matrixone)", grafanaURL)

			case "k8s":
				creator = dashboard.NewK8SDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				logutil.Infof("Deleting K8S dashboards at %s (folder: Matrixone)", grafanaURL)

			case "cloud-ctrl":
				creator = dashboard.NewCloudCtrlPlaneDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				logutil.Infof("Deleting cloud control-plane dashboards at %s (folder: Matrixone)", grafanaURL)

			default:
				return moerr.NewInternalErrorNoCtxf("invalid mode '%s'. Valid modes: local, cloud, k8s, cloud-ctrl", mode)
			}

			if err := creator.Delete(); err != nil {
				errStr := err.Error()
				// Check if it's a warning about skipped provisioned dashboards
				if strings.Contains(errStr, "skipped") && strings.Contains(errStr, "provisioned dashboard") {
					// This is a warning, not a real error - dashboards were deleted successfully
					logutil.Warnf("%v", err)
					logutil.Infof("Dashboards deleted successfully!")
					return nil
				}
				return moerr.NewInternalErrorNoCtxf("failed to delete dashboards: %v", err)
			}

			logutil.Infof("Dashboards deleted successfully!")
			return nil
		},
	}

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List Grafana dashboards for MatrixOne",
		Long: `List Grafana dashboards for MatrixOne monitoring.

This command connects to a Grafana instance and lists all MatrixOne dashboards in the specified folder.
Supports different modes: local (standalone), cloud (docker compose cluster), k8s, and cloud-ctrl.`,
		Example: `  # List local dashboards (default: port 3001)
  mo-tool dashboard list

  # List dashboards for docker compose cluster (port 3000)
  mo-tool dashboard list --mode cloud --port 3000

  # List dashboards with custom host and credentials
  mo-tool dashboard list --host localhost --port 3001 --username admin --password mypass`,
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
				logutil.Infof("Listing local dashboards at %s (folder: Matrixone-Standalone)", grafanaURL)

			case "cloud":
				creator = dashboard.NewCloudDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				logutil.Infof("Listing cloud dashboards at %s (folder: Matrixone)", grafanaURL)

			case "k8s":
				creator = dashboard.NewK8SDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				logutil.Infof("Listing K8S dashboards at %s (folder: Matrixone)", grafanaURL)

			case "cloud-ctrl":
				creator = dashboard.NewCloudCtrlPlaneDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				logutil.Infof("Listing cloud control-plane dashboards at %s (folder: Matrixone)", grafanaURL)

			default:
				return fmt.Errorf("invalid mode '%s'. Valid modes: local, cloud, k8s, cloud-ctrl", mode)
			}

			dashboards, err := creator.List()
			if err != nil {
				return fmt.Errorf("failed to list dashboards: %w", err)
			}

			if len(dashboards) == 0 {
				logutil.Infof("No dashboards found in folder")
				return nil
			}

			logutil.Infof("Found %d dashboard(s):", len(dashboards))
			for i, dash := range dashboards {
				logutil.Infof("  %d. %s (UID: %s)", i+1, dash.Title, dash.UID)
			}

			return nil
		},
	}

	deleteDashboardCmd := &cobra.Command{
		Use:   "delete-dashboard",
		Short: "Delete a single Grafana dashboard by UID",
		Long: `Delete a single Grafana dashboard by its UID.

This command connects to a Grafana instance and deletes the specified dashboard.
You can get the dashboard UID by running the "list" command.`,
		Example: `  # Delete a dashboard by UID (default: port 3001)
  mo-tool dashboard delete-dashboard --uid cf6muzfkdvg1sa

  # Delete a dashboard with custom host and credentials
  mo-tool dashboard delete-dashboard --uid cf6muzfkdvg1sa --host localhost --port 3001 --username admin --password mypass`,
		RunE: func(cmd *cobra.Command, args []string) error {
			grafanaURL := fmt.Sprintf("http://%s:%d", host, port)

			uid, _ := cmd.Flags().GetString("uid")
			if uid == "" {
				return fmt.Errorf("dashboard UID is required. Use --uid flag")
			}

			if err := dashboard.DeleteDashboardByUIDWithAuth(grafanaURL, username, password, uid); err != nil {
				return fmt.Errorf("failed to delete dashboard: %w", err)
			}

			logutil.Infof("Dashboard (UID: %s) deleted successfully!", uid)
			return nil
		},
	}
	deleteDashboardCmd.Flags().String("uid", "", "Dashboard UID to delete (required)")

	deleteFolderCmd := &cobra.Command{
		Use:   "delete-folder",
		Short: "Delete the entire Grafana folder and all dashboards in it",
		Long: `Delete the entire Grafana folder and all dashboards in it.

This command connects to a Grafana instance and deletes the specified folder along with all dashboards inside it.
Supports different modes: local (standalone), cloud (docker compose cluster), k8s, and cloud-ctrl.`,
		Example: `  # Delete local folder (default: port 3001)
  mo-tool dashboard delete-folder

  # Delete folder for docker compose cluster (port 3000)
  mo-tool dashboard delete-folder --mode cloud --port 3000

  # Delete folder with custom host and credentials
  mo-tool dashboard delete-folder --host localhost --port 3001 --username admin --password mypass`,
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
				logutil.Infof("Deleting local folder at %s (folder: Matrixone-Standalone)", grafanaURL)

			case "cloud":
				creator = dashboard.NewCloudDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				logutil.Infof("Deleting cloud folder at %s (folder: Matrixone)", grafanaURL)

			case "k8s":
				creator = dashboard.NewK8SDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				logutil.Infof("Deleting K8S folder at %s (folder: Matrixone)", grafanaURL)

			case "cloud-ctrl":
				creator = dashboard.NewCloudCtrlPlaneDashboardCreator(
					grafanaURL,
					username,
					password,
					datasource,
					"Matrixone",
				)
				logutil.Infof("Deleting cloud control-plane folder at %s (folder: Matrixone)", grafanaURL)

			default:
				return moerr.NewInternalErrorNoCtxf("invalid mode '%s'. Valid modes: local, cloud, k8s, cloud-ctrl", mode)
			}

			if err := creator.DeleteFolder(); err != nil {
				errStr := err.Error()
				// Check if it's a warning about skipped provisioned dashboards
				if strings.Contains(errStr, "skipped") && strings.Contains(errStr, "provisioned dashboard") {
					// This is a warning, not a real error - folder was deleted successfully
					logutil.Warnf("%v", err)
					logutil.Infof("Folder deleted successfully!")
					return nil
				}
				return moerr.NewInternalErrorNoCtxf("failed to delete folder: %v", err)
			}

			logutil.Infof("Folder deleted successfully!")
			return nil
		},
	}

	// Add flags to all commands
	for _, subCmd := range []*cobra.Command{createCmd, deleteCmd, listCmd, deleteDashboardCmd, deleteFolderCmd} {
		subCmd.Flags().StringVar(&host, "host", "127.0.0.1", "Grafana host address")
		subCmd.Flags().IntVar(&port, "port", 3001, "Grafana port (default: 3001 for local, 3000 for docker compose)")
		subCmd.Flags().StringVar(&mode, "mode", "local", "Dashboard mode: local, cloud, k8s, cloud-ctrl (default: local)")
		subCmd.Flags().StringVar(&username, "username", "admin", "Grafana username (default: admin)")
		subCmd.Flags().StringVar(&password, "password", "admin", "Grafana password (default: admin)")
		subCmd.Flags().StringVar(&datasource, "datasource", "Prometheus", "Prometheus datasource name (for cloud/k8s modes, default: Prometheus)")
	}

	cmd.AddCommand(createCmd, deleteCmd, listCmd, deleteDashboardCmd, deleteFolderCmd)

	// For backward compatibility, make "create" the default action
	cmd.RunE = createCmd.RunE
	cmd.Flags().StringVar(&host, "host", "127.0.0.1", "Grafana host address")
	cmd.Flags().IntVar(&port, "port", 3001, "Grafana port (default: 3001 for local, 3000 for docker compose)")
	cmd.Flags().StringVar(&mode, "mode", "local", "Dashboard mode: local, cloud, k8s, cloud-ctrl (default: local)")
	cmd.Flags().StringVar(&username, "username", "admin", "Grafana username (default: admin)")
	cmd.Flags().StringVar(&password, "password", "admin", "Grafana password (default: admin)")
	cmd.Flags().StringVar(&datasource, "datasource", "Prometheus", "Prometheus datasource name (for cloud/k8s modes, default: Prometheus)")

	return cmd
}
