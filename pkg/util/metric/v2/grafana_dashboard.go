package v2

import (
	"context"
	"net/http"

	"github.com/K-Phoen/grabana"
	"github.com/K-Phoen/grabana/axis"
	"github.com/K-Phoen/grabana/dashboard"
	"github.com/K-Phoen/grabana/graph"
	"github.com/K-Phoen/grabana/row"
	"github.com/K-Phoen/grabana/target/prometheus"
	"github.com/K-Phoen/grabana/variable/interval"
)

var (
	txnFolderName     = "Transactions"
	logtailFolderName = "LogTail"
	sqlFolderName     = "SQL"
	fsFolderName      = "FS"
)

type DashboardCreator struct {
	cli        *grabana.Client
	dataSource string
}

func NewDashboardCreator(
	host,
	username,
	password,
	dataSource string) *DashboardCreator {
	return &DashboardCreator{
		cli:        grabana.NewClient(http.DefaultClient, host, grabana.WithBasicAuth(username, password)),
		dataSource: dataSource,
	}
}

func (c *DashboardCreator) Create() error {
	if err := c.initTxnDashboard(); err != nil {
		return err
	}

	if err := c.initLogTailDashboard(); err != nil {
		return err
	}

	if err := c.initSQLDashboard(); err != nil {
		return err
	}

	return c.initFSDashboard()
}

func (c *DashboardCreator) initTxnDashboard() error {
	folder, err := c.createFolder(txnFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"Transaction Status",
		dashboard.AutoRefresh("5s"),
		dashboard.VariableAsInterval(
			"interval",
			interval.Values([]string{"1s", "3s", "5s", "30s", "1m", "5m", "10m", "30m", "1h", "6h", "12h"}),
		),
		c.initTxnTPSRow(),
		c.initTxnStatementRow(),
		c.initTxnCNCommitCostRow(),
		c.initTxnTNCommitCostRow(),
		c.initTxnWaitActiveRow(),
		c.initTxnWaitSnapshotRow(),
		c.initTxnRangesLoadCostRow(),
		c.initTxnTNCommitInQueueCostRow(),
		c.initTxnLockRow(),
		c.initTxnUnlockRow(),
		c.initTxnSendCommitCostRow(),
		c.initTxnTotalCostRow())
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initLogTailDashboard() error {
	folder, err := c.createFolder(logtailFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"LogTail Status",
		dashboard.AutoRefresh("5s"),
		dashboard.VariableAsInterval(
			"interval",
			interval.Values([]string{"1s", "3s", "5s", "30s", "1m", "5m", "10m", "30m", "1h", "6h", "12h"}),
		),
		c.initLogTailTPSRow(),
		c.initLogTailQueueRow(),
		c.initLogTailApplyCostRow(),
		c.initWaitLogTailCostRow(),
		c.initWriteLogTailCostRow(),
		c.initSendLogTailLatencyRow(),
		c.initSendLogTailCostRow(),
		c.initSendLogTailNetworkCostRow(),
		c.initWriteLogTailBytesRow())
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initSQLDashboard() error {
	folder, err := c.createFolder(sqlFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"SQL Status",
		dashboard.AutoRefresh("5s"),
		dashboard.VariableAsInterval(
			"interval",
			interval.Values([]string{"1s", "3s", "5s", "30s", "1m", "5m", "10m", "30m", "1h", "6h", "12h"}),
		),
		c.initSQLBuildCostRow(),
		c.initSQLRunCostRow())
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) initFSDashboard() error {
	folder, err := c.createFolder(fsFolderName)
	if err != nil {
		return err
	}

	build, err := dashboard.New(
		"FileService Status",
		dashboard.AutoRefresh("5s"),
		dashboard.VariableAsInterval(
			"interval",
			interval.Values([]string{"1s", "3s", "5s", "30s", "1m", "5m", "10m", "30m", "1h", "6h", "12h"}),
		),
		c.initFSCountRow(),
		c.initS3ReadBytesRow(),
		c.initS3WriteBytesRow(),
		c.initLocalReadBytesRow(),
		c.initLocalWriteBytesRow(),
		c.initS3ReadIOCostRow(),
		c.initS3WriteIOCostRow(),
		c.initLocalReadIOCostRow(),
		c.initLocalWriteIOCostRow(),
		c.initS3ConnectCostRow(),
		c.initS3GetConnCostRow(),
		c.initS3DNSResolveCostRow(),
		c.initS3TLSHandleShakeCostRow())
	if err != nil {
		return err
	}
	_, err = c.cli.UpsertDashboard(context.Background(), folder, build)
	return err
}

func (c *DashboardCreator) createFolder(name string) (*grabana.Folder, error) {
	folder, err := c.cli.GetFolderByTitle(context.Background(), name)
	if err != nil && err != grabana.ErrFolderNotFound {
		return nil, err
	}

	if folder == nil {
		folder, err = c.cli.CreateFolder(context.Background(), name)
		if err != nil {
			return nil, err
		}
	}
	return folder, nil
}

func (c *DashboardCreator) initTxnStatementRow() dashboard.Option {
	return dashboard.Row(
		"Statement Status",
		c.withGraph(
			"Statement tps",
			6,
			"sum(rate(cn_txn_statement_total[$interval]))",
			""),

		c.withGraph(
			"Statement Retry tps",
			6,
			"sum(rate(cn_txn_statement_retry_total[$interval]))",
			""),
	)
}

func (c *DashboardCreator) initTxnTPSRow() dashboard.Option {
	return dashboard.Row(
		"Txn Status",
		c.withGraph(
			"Txn tps",
			4,
			"sum(rate(cn_txn_txn_total[$interval])) by (type)",
			"{{ type }}"),

		c.withGraph(
			"Commit tps",
			4,
			"sum(rate(tn_txn_handle_commit_total[$interval]))",
			""),

		c.withGraph(
			"Handle Commit Queue",
			4,
			"sum(cn_txn_handle_request_queue_size) by (instance)",
			"{{ instance }}"),
	)
}

func (c *DashboardCreator) initTxnWaitSnapshotRow() dashboard.Option {
	return dashboard.Row(
		"Txn Determine Snapshot",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_txn_determine_snapshot_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_txn_determine_snapshot_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_txn_determine_snapshot_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_txn_determine_snapshot_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTxnWaitActiveRow() dashboard.Option {
	return dashboard.Row(
		"Txn Wait Active",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_txn_wait_active_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_txn_wait_active_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_txn_wait_active_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_txn_wait_active_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTxnLockRow() dashboard.Option {
	return dashboard.Row(
		"Txn Lock Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_txn_lock_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_txn_lock_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_txn_lock_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_txn_lock_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTxnUnlockRow() dashboard.Option {
	return dashboard.Row(
		"Txn Unlock Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_txn_unlock_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_txn_unlock_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_txn_unlock_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_txn_unlock_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTxnTotalCostRow() dashboard.Option {
	return dashboard.Row(
		"Txn Total Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_txn_total_cost_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_txn_total_cost_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_txn_total_cost_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_txn_total_cost_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTxnCNCommitCostRow() dashboard.Option {
	return dashboard.Row(
		"Txn CN Commit Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_txn_commit_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_txn_commit_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_txn_commit_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_txn_commit_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTxnTNCommitCostRow() dashboard.Option {
	return dashboard.Row(
		"Txn TN Commit Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(tn_txn_handle_commit_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(tn_txn_handle_commit_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(tn_txn_handle_commit_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(tn_txn_handle_commit_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTxnTNCommitInQueueCostRow() dashboard.Option {
	return dashboard.Row(
		"Txn TN Commit In Queue Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(tn_txn_handle_queue_in_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(tn_txn_handle_queue_in_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(tn_txn_handle_queue_in_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(tn_txn_handle_queue_in_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTxnSendCommitCostRow() dashboard.Option {
	return dashboard.Row(
		"Txn Send Commit Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_txn_send_request_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_txn_send_request_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_txn_send_request_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_txn_send_request_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initTxnRangesLoadCostRow() dashboard.Option {
	return dashboard.Row(
		"Txn Ranges Load Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_txn_ranges_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_txn_ranges_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_txn_ranges_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_txn_ranges_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initSQLBuildCostRow() dashboard.Option {
	return dashboard.Row(
		"SQL Build Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_sql_build_plan_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_sql_build_plan_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_sql_build_plan_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_sql_build_plan_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initSQLRunCostRow() dashboard.Option {
	return dashboard.Row(
		"SQL Run Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_sql_sql_run_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_sql_sql_run_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_sql_sql_run_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_sql_sql_run_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initLogTailTPSRow() dashboard.Option {
	return dashboard.Row(
		"LogTail Status",
		c.withGraph(
			"Write tps",
			4,
			`sum(rate(tn_logtail_log_tail_bytes_count{type="write"}[$interval])) by (type)`,
			"{{ type }}"),

		c.withGraph(
			"Send tps",
			4,
			`sum(rate(tn_logtail_log_tail_bytes_count{type="send"}[$interval])) by (type)`,
			"{{ type }}"),

		c.withGraph(
			"Receive tps",
			4,
			`sum(rate(tn_logtail_log_tail_bytes_count{type="receive"}[$interval])) by (type)`,
			"{{ type }}"),
	)
}

func (c *DashboardCreator) initLogTailQueueRow() dashboard.Option {
	return dashboard.Row(
		"LogTail Queue Status",
		c.withGraph(
			"Sending Queue",
			6,
			"sum(tn_logtail_sending_queue_size)",
			""),

		c.withGraph(
			"Receiving Queue",
			6,
			"sum(cn_logtail_receive_queue_size)",
			""),
	)
}

func (c *DashboardCreator) initLogTailApplyCostRow() dashboard.Option {
	return dashboard.Row(
		"Logtail Apply Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_logtail_apply_log_tail_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_logtail_apply_log_tail_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_logtail_apply_log_tail_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_logtail_apply_log_tail_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initWaitLogTailCostRow() dashboard.Option {
	return dashboard.Row(
		"Logtail Wait Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_logtail_wait_log_tail_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_logtail_wait_log_tail_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_logtail_wait_log_tail_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_logtail_wait_log_tail_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initWriteLogTailCostRow() dashboard.Option {
	return dashboard.Row(
		"Logtail Write Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(tn_logtail_append_log_tail_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(tn_logtail_append_log_tail_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(tn_logtail_append_log_tail_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(tn_logtail_append_log_tail_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initSendLogTailLatencyRow() dashboard.Option {
	return dashboard.Row(
		"Logtail Send Latency",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(tn_logtail_send_log_tail_latency_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(tn_logtail_send_log_tail_latency_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(tn_logtail_send_log_tail_latency_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(tn_logtail_send_log_tail_latency_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initSendLogTailCostRow() dashboard.Option {
	return dashboard.Row(
		"Logtail Send Total Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(tn_logtail_send_log_tail_duration_seconds_bucket{step="total"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(tn_logtail_send_log_tail_duration_seconds_bucket{step="total"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(tn_logtail_send_log_tail_duration_seconds_bucket{step="total"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(tn_logtail_send_log_tail_duration_seconds_bucket{step="total"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initSendLogTailNetworkCostRow() dashboard.Option {
	return dashboard.Row(
		"Logtail Send Network Step Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(tn_logtail_send_log_tail_duration_seconds_bucket{step="network"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(tn_logtail_send_log_tail_duration_seconds_bucket{step="network"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(tn_logtail_send_log_tail_duration_seconds_bucket{step="network"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(tn_logtail_send_log_tail_duration_seconds_bucket{step="network"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initWriteLogTailBytesRow() dashboard.Option {
	return dashboard.Row(
		"Logtail Write Bytes",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(tn_logtail_log_tail_bytes_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(tn_logtail_log_tail_bytes_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(tn_logtail_log_tail_bytes_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(tn_logtail_log_tail_bytes_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initFSCountRow() dashboard.Option {
	return dashboard.Row(
		"FileService Status",
		c.withGraph(
			"S3 op qps",
			4,
			"sum(rate(cn_fs_s3_io_bytes_count[$interval])) by (type)",
			"{{ type }}"),

		c.withGraph(
			"S3 Connect qps",
			4,
			"sum(rate(cn_fs_s3_connect_total[$interval]))",
			""),

		c.withGraph(
			"S3 DNS Resolve qps",
			4,
			"sum(rate(cn_fs_s3_dns_resolve_total[$interval]))",
			""),
	)
}

func (c *DashboardCreator) initS3ReadBytesRow() dashboard.Option {
	return dashboard.Row(
		"S3 Read Bytes",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_fs_s3_io_bytes_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_fs_s3_io_bytes_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_fs_s3_io_bytes_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_fs_s3_io_bytes_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initS3WriteBytesRow() dashboard.Option {
	return dashboard.Row(
		"S3 Write Bytes",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_fs_s3_io_bytes_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_fs_s3_io_bytes_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_fs_s3_io_bytes_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_fs_s3_io_bytes_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initLocalReadBytesRow() dashboard.Option {
	return dashboard.Row(
		"Local Read Bytes",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_fs_local_io_bytes_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_fs_local_io_bytes_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_fs_local_io_bytes_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_fs_local_io_bytes_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initLocalWriteBytesRow() dashboard.Option {
	return dashboard.Row(
		"Local Write Bytes",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_fs_local_io_bytes_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_fs_local_io_bytes_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_fs_local_io_bytes_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_fs_local_io_bytes_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("bytes"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initS3ReadIOCostRow() dashboard.Option {
	return dashboard.Row(
		"S3 Read Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_fs_s3_io_duration_seconds_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_fs_s3_io_duration_seconds_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_fs_s3_io_duration_seconds_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_fs_s3_io_duration_seconds_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initS3WriteIOCostRow() dashboard.Option {
	return dashboard.Row(
		"S3 Write Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_fs_s3_io_duration_seconds_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_fs_s3_io_duration_seconds_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_fs_s3_io_duration_seconds_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_fs_s3_io_duration_seconds_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initLocalReadIOCostRow() dashboard.Option {
	return dashboard.Row(
		"Local Read Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_fs_local_io_duration_seconds_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_fs_local_io_duration_seconds_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_fs_local_io_duration_seconds_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_fs_local_io_duration_seconds_bucket{type="read"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initLocalWriteIOCostRow() dashboard.Option {
	return dashboard.Row(
		"Local Write Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_fs_local_io_duration_seconds_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_fs_local_io_duration_seconds_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_fs_local_io_duration_seconds_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_fs_local_io_duration_seconds_bucket{type="write"}[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initS3TLSHandleShakeCostRow() dashboard.Option {
	return dashboard.Row(
		"S3 TLS HandleShake Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_fs_s3_tls_handshake_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_fs_s3_tls_handshake_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_fs_s3_tls_handshake_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_fs_s3_tls_handshake_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initS3GetConnCostRow() dashboard.Option {
	return dashboard.Row(
		"S3 Get Conn Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_fs_s3_conn_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_fs_s3_conn_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_fs_s3_conn_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_fs_s3_conn_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initS3ConnectCostRow() dashboard.Option {
	return dashboard.Row(
		"S3 Connect Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_fs_s3_connect_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_fs_s3_connect_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_fs_s3_connect_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_fs_s3_connect_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) initS3DNSResolveCostRow() dashboard.Option {
	return dashboard.Row(
		"S3 DNS Resolve Cost",

		c.withGraph(
			"80% time",
			3,
			`histogram_quantile(0.80, sum(rate(cn_fs_s3_dns_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"90% time",
			3,
			`histogram_quantile(0.90, sum(rate(cn_fs_s3_dns_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99% time",
			3,
			`histogram_quantile(0.99, sum(rate(cn_fs_s3_dns_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),

		c.withGraph(
			"99.99% time",
			3,
			`histogram_quantile(0.9999, sum(rate(cn_fs_s3_dns_duration_seconds_bucket[$interval])) by (le, instance))`,
			"{{ instance }}",
			axis.Unit("s"),
			axis.Min(0)),
	)
}

func (c *DashboardCreator) withGraph(
	title string,
	span float32,
	pql string,
	legend string,
	opts ...axis.Option) row.Option {
	return row.WithGraph(
		title,
		graph.Span(span),
		graph.Height("400px"),
		graph.DataSource(c.dataSource),
		graph.WithPrometheusTarget(
			pql,
			prometheus.Legend(legend),
		),
		graph.LeftYAxis(opts...),
	)
}
