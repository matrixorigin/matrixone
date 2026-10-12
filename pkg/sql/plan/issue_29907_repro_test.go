package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	statsinfo "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type issue29907CompilerContext struct {
	*MockCompilerContext
	stats map[string]*statsinfo.StatsInfo
	cache *StatsCache
}

func (c *issue29907CompilerContext) Stats(obj *planpb.ObjectRef, _ *Snapshot) (*statsinfo.StatsInfo, error) {
	if obj == nil {
		return nil, nil
	}
	return c.stats[obj.ObjName], nil
}

func (c *issue29907CompilerContext) GetStatsCache() *StatsCache {
	return c.cache
}

type issue29907Optimizer struct {
	ctx *issue29907CompilerContext
}

func (o *issue29907Optimizer) CurrentContext() CompilerContext { return o.ctx }

func (o *issue29907Optimizer) Optimize(stmt tree.Statement) (*Query, error) {
	return NewBaseOptimizer(o.ctx).Optimize(stmt, false)
}

func addIssueTable(ctx *MockCompilerContext, name string, cols []struct {
	name string
	typ  types.T
}) {
	base := DeepCopyTableDef(ctx.tables["nation"], true)
	base.Name = name
	base.DbName = "tpch"
	base.TblId += uint64(len(ctx.tables) + 1000)
	base.Pkey = nil
	base.Indexes = nil
	base.Cols = make([]*ColDef, 0, len(cols))
	base.Name2ColIndex = make(map[string]int32, len(cols))
	for i, col := range cols {
		base.Cols = append(base.Cols, &ColDef{
			ColId: uint64(i),
			Name:  col.name,
			Typ: planpb.Type{
				Id:          int32(col.typ),
				NotNullable: false,
				Width:       64,
			},
			Default: &planpb.Default{NullAbility: true},
		})
		base.Name2ColIndex[col.name] = int32(i)
	}
	switch name {
	case "task_instance":
		base.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "id", Cols: []uint64{0}, Names: []string{"id"}, CompPkeyCol: base.Cols[0]}
		base.Indexes = []*planpb.IndexDef{
			{IndexName: "ti_state", Parts: []string{"state"}, IndexTableName: "ti_state", TableExist: true},
			{IndexName: "ti_dag_run", Parts: []string{"dag_id", "run_id"}, IndexTableName: "ti_dag_run", TableExist: true},
		}
	case "dag":
		base.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "dag_id", Cols: []uint64{0}, Names: []string{"dag_id"}, CompPkeyCol: base.Cols[0]}
	case "dag_run":
		base.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "id", Cols: []uint64{0}, Names: []string{"id"}, CompPkeyCol: base.Cols[0]}
		base.Indexes = []*planpb.IndexDef{
			{IndexName: "dag_id", Parts: []string{"dag_id", "run_id"}, Unique: true, IndexTableName: "dag_id", TableExist: true},
		}
	}
	obj := *ctx.objects["nation"]
	obj.Obj = int64(base.TblId)
	obj.ObjName = name
	obj.SchemaName = "tpch"
	ctx.tables[name] = base
	ctx.objects[name] = &obj
}

func TestIssue29907AirflowJoinColumnRemapping(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	addIssueTable(ctx, "task_instance", []struct {
		name string
		typ  types.T
	}{
		{"id", types.T_varchar}, {"task_id", types.T_varchar}, {"dag_id", types.T_varchar},
		{"run_id", types.T_varchar}, {"map_index", types.T_int64}, {"start_date", types.T_timestamp},
		{"end_date", types.T_timestamp}, {"duration", types.T_float64}, {"state", types.T_varchar},
		{"try_number", types.T_int64}, {"max_tries", types.T_int64}, {"hostname", types.T_varchar},
		{"unixname", types.T_varchar}, {"pool", types.T_varchar}, {"pool_slots", types.T_int64},
		{"queue", types.T_varchar}, {"priority_weight", types.T_int64}, {"operator", types.T_varchar},
		{"custom_operator_name", types.T_varchar}, {"queued_dttm", types.T_timestamp},
		{"scheduled_dttm", types.T_timestamp}, {"queued_by_job_id", types.T_int64},
		{"last_heartbeat_at", types.T_timestamp}, {"pid", types.T_int64}, {"executor", types.T_varchar},
		{"executor_config", types.T_blob}, {"updated_at", types.T_timestamp},
		{"rendered_map_index", types.T_varchar}, {"context_carrier", types.T_json},
		{"span_status", types.T_varchar}, {"external_executor_id", types.T_varchar},
		{"trigger_id", types.T_int64}, {"trigger_timeout", types.T_timestamp},
		{"next_method", types.T_varchar}, {"next_kwargs", types.T_json},
		{"task_display_name", types.T_varchar}, {"dag_version_id", types.T_varchar},
		{"retry_delay_override", types.T_float64}, {"retry_reason", types.T_varchar},
	})
	addIssueTable(ctx, "dag", []struct {
		name string
		typ  types.T
	}{{"dag_id", types.T_varchar}})
	addIssueTable(ctx, "dag_run", []struct {
		name string
		typ  types.T
	}{{"id", types.T_int64}, {"dag_id", types.T_varchar}, {"queued_at", types.T_timestamp},
		{"logical_date", types.T_timestamp}, {"start_date", types.T_timestamp}, {"end_date", types.T_timestamp},
		{"state", types.T_varchar}, {"run_id", types.T_varchar}, {"creating_job_id", types.T_int64},
		{"run_type", types.T_varchar}, {"triggered_by", types.T_varchar}, {"triggering_user_name", types.T_varchar},
		{"conf", types.T_json}, {"data_interval_start", types.T_timestamp}, {"data_interval_end", types.T_timestamp},
		{"run_after", types.T_timestamp}, {"last_scheduling_decision", types.T_timestamp},
		{"log_template_id", types.T_int64}, {"created_at", types.T_timestamp}, {"updated_at", types.T_timestamp},
		{"clear_number", types.T_int64}, {"backfill_id", types.T_int64}, {"bundle_version", types.T_varchar},
		{"scheduled_by_job_id", types.T_int64}, {"context_carrier", types.T_json}, {"span_status", types.T_varchar},
		{"created_dag_version_id", types.T_varchar}, {"partition_key", types.T_varchar}, {"partition_date", types.T_timestamp}})
	stats := map[string]*statsinfo.StatsInfo{
		"task_instance": {TableName: "task_instance", TableCnt: 482, NdvMap: map[string]float64{
			"id": 482, "dag_id": 1, "run_id": 241, "state": 2, "last_heartbeat_at": 482, "queued_by_job_id": 10,
		}},
		"dag": {TableName: "dag", TableCnt: 1, NdvMap: map[string]float64{"dag_id": 1}},
		"dag_run": {TableName: "dag_run", TableCnt: 241, NdvMap: map[string]float64{
			"id": 241, "dag_id": 1, "run_id": 241,
		}},
	}
	cache := NewStatsCache()
	for name, info := range stats {
		cache.Set(ctx.tables[name].TblId, info)
	}
	opt := &issue29907Optimizer{ctx: &issue29907CompilerContext{MockCompilerContext: ctx, stats: stats, cache: cache}}
	queries := []string{`
		SELECT task_instance.id, task_instance.task_id, task_instance.dag_id, task_instance.run_id,
		       task_instance.map_index, task_instance.start_date, task_instance.end_date,
		       task_instance.duration, task_instance.state, task_instance.try_number, task_instance.max_tries,
		       task_instance.hostname, task_instance.unixname, task_instance.pool, task_instance.pool_slots,
		       task_instance.queue, task_instance.priority_weight, task_instance.operator,
		       task_instance.custom_operator_name, task_instance.queued_dttm, task_instance.scheduled_dttm,
		       task_instance.queued_by_job_id, task_instance.last_heartbeat_at, task_instance.pid,
		       task_instance.executor, task_instance.executor_config, task_instance.updated_at,
		       task_instance.rendered_map_index, task_instance.context_carrier, task_instance.span_status,
		       task_instance.external_executor_id, task_instance.trigger_id, task_instance.trigger_timeout,
		       task_instance.next_method, task_instance.next_kwargs, task_instance.task_display_name,
		       task_instance.dag_version_id, task_instance.retry_delay_override, task_instance.retry_reason,
		       dag_run_1.id AS id_1, dag_run_1.dag_id AS dag_id_1, dag_run_1.queued_at, dag_run_1.logical_date,
		       dag_run_1.start_date AS start_date_1, dag_run_1.end_date AS end_date_1, dag_run_1.state AS state_1,
		       dag_run_1.run_id AS run_id_1, dag_run_1.creating_job_id, dag_run_1.run_type, dag_run_1.triggered_by,
		       dag_run_1.triggering_user_name, dag_run_1.conf, dag_run_1.data_interval_start, dag_run_1.data_interval_end,
		       dag_run_1.run_after, dag_run_1.last_scheduling_decision, dag_run_1.log_template_id, dag_run_1.created_at,
		       dag_run_1.updated_at AS updated_at_1, dag_run_1.clear_number, dag_run_1.backfill_id, dag_run_1.bundle_version,
		       dag_run_1.scheduled_by_job_id, dag_run_1.context_carrier AS context_carrier_1, dag_run_1.span_status AS span_status_1,
		       dag_run_1.created_dag_version_id, dag_run_1.partition_key, dag_run_1.partition_date
		FROM tpch.task_instance
		INNER JOIN tpch.dag ON task_instance.dag_id = dag.dag_id
		INNER JOIN tpch.dag_run AS dag_run_1 ON dag_run_1.dag_id = task_instance.dag_id
		                         AND dag_run_1.run_id = task_instance.run_id
		WHERE task_instance.state IN ('running', 'restarting')
		  AND task_instance.last_heartbeat_at < '2026-10-11 10:00:26.268106'
		  AND task_instance.queued_by_job_id = 8`, `
		SELECT task_instance.id, task_instance.task_id, task_instance.dag_id, task_instance.run_id,
		       task_instance.map_index, task_instance.start_date, task_instance.end_date,
		       task_instance.duration, task_instance.state, task_instance.try_number, task_instance.max_tries,
		       task_instance.hostname, task_instance.unixname, task_instance.pool, task_instance.pool_slots,
		       task_instance.queue, task_instance.priority_weight, task_instance.operator,
		       task_instance.custom_operator_name, task_instance.queued_dttm, task_instance.scheduled_dttm,
		       task_instance.queued_by_job_id, task_instance.last_heartbeat_at, task_instance.pid,
		       task_instance.executor, task_instance.executor_config, task_instance.updated_at,
		       task_instance.rendered_map_index, task_instance.context_carrier, task_instance.span_status,
		       task_instance.external_executor_id, task_instance.trigger_id, task_instance.trigger_timeout,
		       task_instance.next_method, task_instance.next_kwargs, task_instance.task_display_name,
		       task_instance.dag_version_id, task_instance.retry_delay_override, task_instance.retry_reason,
		       dag_run_1.id AS id_1, dag_run_1.dag_id AS dag_id_1, dag_run_1.queued_at, dag_run_1.logical_date,
		       dag_run_1.start_date AS start_date_1, dag_run_1.end_date AS end_date_1, dag_run_1.state AS state_1,
		       dag_run_1.run_id AS run_id_1, dag_run_1.creating_job_id, dag_run_1.run_type, dag_run_1.triggered_by,
		       dag_run_1.triggering_user_name, dag_run_1.conf, dag_run_1.data_interval_start, dag_run_1.data_interval_end,
		       dag_run_1.run_after, dag_run_1.last_scheduling_decision, dag_run_1.log_template_id, dag_run_1.created_at,
		       dag_run_1.updated_at AS updated_at_1, dag_run_1.clear_number, dag_run_1.backfill_id, dag_run_1.bundle_version,
		       dag_run_1.scheduled_by_job_id, dag_run_1.context_carrier AS context_carrier_1, dag_run_1.span_status AS span_status_1,
		       dag_run_1.created_dag_version_id, dag_run_1.partition_key, dag_run_1.partition_date
		FROM tpch.task_instance USE INDEX (ti_state)
		INNER JOIN tpch.dag ON task_instance.dag_id = dag.dag_id
		INNER JOIN tpch.dag_run AS dag_run_1 ON dag_run_1.dag_id = task_instance.dag_id
		                         AND dag_run_1.run_id = task_instance.run_id
		WHERE task_instance.state IN ('running', 'restarting')
		  AND task_instance.last_heartbeat_at < '2026-10-11 10:00:26.268106'
		  AND task_instance.queued_by_job_id = 8`}
	for i, sql := range queries {
		name := "without index hint"
		if i == 1 {
			name = "with index hint"
		}
		t.Run(name, func(t *testing.T) {
			stmts, err := mysql.Parse(opt.CurrentContext().GetContext(), sql, 1)
			require.NoError(t, err)
			query, err := opt.Optimize(stmts[0])
			stmts[0].Free()
			require.NoError(t, err)
			require.NotNil(t, query)
			require.Len(t, query.Steps, 1)
			require.Equal(t, planpb.Node_PROJECT, query.Nodes[query.Steps[0]].NodeType)
		})
	}
}
