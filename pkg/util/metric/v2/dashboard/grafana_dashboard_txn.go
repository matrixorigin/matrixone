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
	"context"

	"github.com/K-Phoen/grabana/dashboard"
)

func (c *DashboardCreator) initTxnDashboard() error {
	folder, err := c.createFolder(txnFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Txn Metrics",
		c.withRowOptions(
			c.initTxnOverviewRow(),
			c.initTxnLifeRow(),
			c.initTxnCreateRow(),
			c.initTxnDetermineSnapshotRow(),
			c.initTxnWaitActiveRow(),
			c.initTxnQueueRow(),
			c.initTxnCNCommitRow(),
			c.initTxnCNSendCommitRow(),
			c.initTxnCNReceiveCommitResponseRow(),
			c.initTxnCNWaitCommitLogtailResponseRow(),
			c.initTxnTNCommitRow(),
			c.initTxnBuildPlanRow(),
			c.initTxnStatementExecuteRow(),
			c.initTxnAcquireLockRow(),
			c.initTxnHoldLockRow(),
			c.initTxnUnlockRow(),
			c.initTxnTableRangesRow(),
			c.initTxnPrePrepareRow(),
		)...)
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initTxnOverviewRow() dashboard.Option {
	return dashboard.Row(
		"Txn overview",
		c.withGraph(
			"Txn requests",
			3,
			`sum(rate(mo_txn_total{matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}[$interval])) by (pod, type)`,
			"{{ pod-type }}"),

		c.withGraph(
			"Statement requests",
			3,
			`sum(rate(mo_txn_statement_total{matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}[$interval])) by (pod, type)`,
			"{{ pod-type }}"),

		c.withGraph(
			"Commit requests",
			3,
			`sum(rate(mo_txn_commit_total{matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}[$interval])) by (pod, type)`,
			"{{ pod-type }}"),

		c.withGraph(
			"Rollback requests",
			3,
			`sum(rate(mo_txn_rollback_total{matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}[$interval])) by (pod)`,
			"{{ pod }}"),
	)
}

func (c *DashboardCreator) initTxnPrePrepareRow() dashboard.Option {
	return dashboard.Row(
		"txn pre-prepare duration",
		c.getHistogram(
			`mo_txn_pre_prepare_duration_seconds_bucket{matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnCNCommitRow() dashboard.Option {
	return dashboard.Row(
		"Txn CN Commit",
		c.getHistogram(
			`mo_txn_commit_duration_seconds_bucket{type="cn", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnCNSendCommitRow() dashboard.Option {
	return dashboard.Row(
		"Txn CN Send Commit Request",
		c.getHistogram(
			`mo_txn_commit_duration_seconds_bucket{type="cn-send", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnCNReceiveCommitResponseRow() dashboard.Option {
	return dashboard.Row(
		"Txn CN receive commit response",
		c.getHistogram(
			`mo_txn_commit_duration_seconds_bucket{type="cn-resp", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnCNWaitCommitLogtailResponseRow() dashboard.Option {
	return dashboard.Row(
		"Txn CN wait commit logtail",
		c.getHistogram(
			`mo_txn_commit_duration_seconds_bucket{type="cn-wait-logtail", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnTNCommitRow() dashboard.Option {
	return dashboard.Row(
		"Txn TN commit",
		c.getHistogram(
			`mo_txn_commit_duration_seconds_bucket{type="tn", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnLifeRow() dashboard.Option {
	return dashboard.Row(
		"Txn life",
		c.getHistogram(
			`mo_txn_life_duration_seconds_bucket{matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnCreateRow() dashboard.Option {
	return dashboard.Row(
		"Txn create",
		c.getHistogram(
			`mo_txn_create_duration_seconds_bucket{type="total", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnQueueRow() dashboard.Option {
	return dashboard.Row(
		"Txn Queue Status",
		c.withGraph(
			"Txn Active Queue",
			4,
			`sum(mo_txn_queue_size{type="active", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"})`,
			""),
		c.withGraph(
			"Txn Wait Active Queue",
			4,
			`sum(mo_txn_queue_size{type="wait-active", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"})`,
			""),

		c.withGraph(
			"TN Commit Queue",
			4,
			`sum(mo_txn_queue_size{type="commit", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"})`,
			""),
	)
}

func (c *DashboardCreator) initTxnDetermineSnapshotRow() dashboard.Option {
	return dashboard.Row(
		"Txn determine snapshot",
		c.getHistogram(
			`mo_txn_create_duration_seconds_bucket{type="determine-snapshot", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnWaitActiveRow() dashboard.Option {
	return dashboard.Row(
		"Txn wait active",
		c.getHistogram(
			`mo_txn_create_duration_seconds_bucket{type="wait-active", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnBuildPlanRow() dashboard.Option {
	return dashboard.Row(
		"Txn build plan",
		c.getHistogram(
			`mo_txn_statement_duration_seconds_bucket{type="build-plan", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnStatementExecuteRow() dashboard.Option {
	return dashboard.Row(
		"Txn execute statement",
		c.getHistogram(
			`mo_txn_statement_duration_seconds_bucket{type="execute", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnTableRangesRow() dashboard.Option {
	return dashboard.Row(
		"Txn execute table ranges",
		c.getHistogram(
			`mo_txn_ranges_duration_seconds_bucket{matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnAcquireLockRow() dashboard.Option {
	return dashboard.Row(
		"Txn Acquire Lock",
		c.getHistogram(
			`mo_txn_lock_duration_seconds_bucket{type="acquire", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnUnlockRow() dashboard.Option {
	return dashboard.Row(
		"Txn Release Lock",
		c.getHistogram(
			`mo_txn_unlock_duration_seconds_bucket{matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnHoldLockRow() dashboard.Option {
	return dashboard.Row(
		"Txn Hold Lock",
		c.getHistogram(
			`mo_txn_lock_duration_seconds_bucket{type="hold", matrixone_cloud_main_cluster=~"$physicalCluster", matrixone_cloud_cluster=~"$cluster", pod=~"$pod"}`,
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}
