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
	folder, err := c.createFolder(moFolderName)
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
			c.initTxnOnPrepareWALRow(),
			c.initTxnBeforeCommitRow(),
			c.initTxnDequeuePreparedRow(),
			c.initTxnDequeuePreparingRow(),
			c.initTxnFastLoadObjectMetaRow(),
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
			2.4,
			`sum(rate(`+c.getMetricWithFilter("mo_txn_total", "")+`[$interval])) by (`+c.by+`, type)`,
			"{{ "+c.by+"-type }}"),

		c.withGraph(
			"Statement requests",
			2.4,
			`sum(rate(`+c.getMetricWithFilter("mo_txn_statement_total", "")+`[$interval])) by (`+c.by+`, type)`,
			"{{ "+c.by+"-type }}"),

		c.withGraph(
			"Commit requests",
			2.4,
			`sum(rate(`+c.getMetricWithFilter("mo_txn_commit_total", "")+`[$interval])) by (`+c.by+`, type)`,
			"{{ "+c.by+"-type }}"),

		c.withGraph(
			"Rollback requests",
			2.4,
			`sum(rate(`+c.getMetricWithFilter("mo_txn_rollback_total", "")+`[$interval])) by (`+c.by+`)`,
			"{{ "+c.by+" }}"),

		c.withGraph(
			"Lock requests",
			2.4,
			`sum(rate(`+c.getMetricWithFilter("mo_txn_lock_total", "")+`[$interval])) by (`+c.by+`, type)`,
			"{{ "+c.by+"-type }}"),
	)
}

func (c *DashboardCreator) initTxnOnPrepareWALRow() dashboard.Option {
	return dashboard.Row(
		"txn on prepare wal duration",
		c.getHistogram(
			c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="on_prepare_wal"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnDequeuePreparingRow() dashboard.Option {
	return dashboard.Row(
		"txn dequeue preparing duration",
		c.getHistogram(
			c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="dequeue_preparing"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnDequeuePreparedRow() dashboard.Option {
	return dashboard.Row(
		"txn dequeue prepared duration",
		c.getHistogram(
			c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="dequeue_prepared"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnBeforeCommitRow() dashboard.Option {
	return dashboard.Row(
		"txn handle commit but before txn.commit duration",
		c.getHistogram(
			c.getMetricWithFilter("mo_txn_tn_side_duration_seconds_bucket", `step="before_txn_commit"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnCNCommitRow() dashboard.Option {
	return dashboard.Row(
		"Txn CN Commit",
		c.getHistogram(
			c.getMetricWithFilter("mo_txn_commit_duration_seconds_bucket", `type="cn"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnCNSendCommitRow() dashboard.Option {
	return dashboard.Row(
		"Txn CN Send Commit Request",
		c.getHistogram(
			c.getMetricWithFilter(`mo_txn_commit_duration_seconds_bucket`, `type="cn-send"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnCNReceiveCommitResponseRow() dashboard.Option {
	return dashboard.Row(
		"Txn CN receive commit response",
		c.getHistogram(
			c.getMetricWithFilter(`mo_txn_commit_duration_seconds_bucket`, `type="cn-resp"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnCNWaitCommitLogtailResponseRow() dashboard.Option {
	return dashboard.Row(
		"Txn CN wait commit logtail",
		c.getHistogram(
			c.getMetricWithFilter(`mo_txn_commit_duration_seconds_bucket`, `type="cn-wait-logtail"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnTNCommitRow() dashboard.Option {
	return dashboard.Row(
		"Txn TN commit",
		c.getHistogram(
			c.getMetricWithFilter(`mo_txn_commit_duration_seconds_bucket`, `type="tn"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnLifeRow() dashboard.Option {
	return dashboard.Row(
		"Txn life",
		c.getHistogram(
			c.getMetricWithFilter(`mo_txn_life_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnCreateRow() dashboard.Option {
	return dashboard.Row(
		"Txn create",
		c.getHistogram(
			c.getMetricWithFilter(`mo_txn_create_duration_seconds_bucket`, `type="total"`),
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
			`sum(`+c.getMetricWithFilter("mo_txn_queue_size", `type="active"`)+`)`,
			""),
		c.withGraph(
			"Txn Wait Active Queue",
			4,
			`sum(`+c.getMetricWithFilter("mo_txn_queue_size", `type="wait-active"`)+`)`,
			""),

		c.withGraph(
			"TN Commit Queue",
			4,
			`sum(`+c.getMetricWithFilter("mo_txn_queue_size", `type="commit"`)+`)`,
			""),
	)
}

func (c *DashboardCreator) initTxnDetermineSnapshotRow() dashboard.Option {
	return dashboard.Row(
		"Txn determine snapshot",
		c.getHistogram(
			c.getMetricWithFilter(`mo_txn_create_duration_seconds_bucket`, `type="determine-snapshot"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnWaitActiveRow() dashboard.Option {
	return dashboard.Row(
		"Txn wait active",
		c.getHistogram(
			c.getMetricWithFilter(`mo_txn_create_duration_seconds_bucket`, `type="wait-active"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnBuildPlanRow() dashboard.Option {
	return dashboard.Row(
		"Txn build plan",
		c.getHistogram(
			c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="build-plan"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnStatementExecuteRow() dashboard.Option {
	return dashboard.Row(
		"Txn execute statement",
		c.getHistogram(
			c.getMetricWithFilter(`mo_txn_statement_duration_seconds_bucket`, `type="execute"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnTableRangesRow() dashboard.Option {
	return dashboard.Row(
		"Txn execute table ranges",
		c.getHistogram(
			c.getMetricWithFilter(`mo_txn_ranges_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnAcquireLockRow() dashboard.Option {
	return dashboard.Row(
		"Txn Acquire Lock",
		c.getHistogram(
			c.getMetricWithFilter(`mo_txn_lock_duration_seconds_bucket`, `type="acquire"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnUnlockRow() dashboard.Option {
	return dashboard.Row(
		"Txn Release Lock",
		c.getHistogram(
			c.getMetricWithFilter(`mo_txn_unlock_duration_seconds_bucket`, ``),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnHoldLockRow() dashboard.Option {
	return dashboard.Row(
		"Txn Hold Lock",
		c.getHistogram(
			c.getMetricWithFilter(`mo_txn_lock_duration_seconds_bucket`, `type="hold"`),
			[]float64{0.50, 0.8, 0.90, 0.99},
			[]float32{3, 3, 3, 3})...,
	)
}

func (c *DashboardCreator) initTxnFastLoadObjectMetaRow() dashboard.Option {
	return dashboard.Row(
		"Txn Fast Load Object Meta",
		c.withGraph(
			"Fast Load Object Meta",
			12,
			`sum(rate(`+c.getMetricWithFilter("tn_side_fast_load_object_meta_total", "")+`[$interval])) by (`+c.by+`, type)`,
			"{{ "+c.by+"-type }}"),
	)
}
