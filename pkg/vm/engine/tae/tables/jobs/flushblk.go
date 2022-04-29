package jobs

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	idxCommon "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/io"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type flushBlkTask struct {
	*tasks.BaseTask
	data *batch.Batch
	meta *catalog.BlockEntry
	file file.Block
	ts   uint64
}

func NewFlushBlkTask(ctx *tasks.Context, bf file.Block, ts uint64, meta *catalog.BlockEntry, data *batch.Batch) *flushBlkTask {
	task := &flushBlkTask{
		ts:   ts,
		data: data,
		meta: meta,
		file: bf,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.IOTask, ctx)
	return task
}

func (task *flushBlkTask) Scope() *common.ID { return task.meta.AsCommonID() }

func (task *flushBlkTask) Execute() (err error) {
	// write indexes, collect their meta, and refresh host's index holder
	schema := task.meta.GetSchema()
	pkColumn, err := task.file.OpenColumn(int(schema.PrimaryKey))
	if err != nil {
		return
	}
	pkColumnData := task.data.Vecs[schema.PrimaryKey]
	zmIdx := uint16(0)
	sfIdx := uint16(1)
	metas := idxCommon.NewEmptyIndicesMeta()

	zoneMapWriter := io.NewBlockZoneMapIndexWriter()
	zmFile, err := pkColumn.OpenIndexFile(int(zmIdx))
	if err != nil {
		return err
	}
	err = zoneMapWriter.Init(zmFile, idxCommon.Plain, uint16(schema.PrimaryKey), zmIdx)
	if err != nil {
		return err
	}
	err = zoneMapWriter.AddValues(pkColumnData)
	if err != nil {
		return err
	}
	meta, err := zoneMapWriter.Finalize()
	if err != nil {
		return err
	}
	metas.AddIndex(*meta)

	staticFilterWriter := io.NewStaticFilterIndexWriter()
	sfFile, err := pkColumn.OpenIndexFile(int(sfIdx))
	if err != nil {
		return err
	}
	err = staticFilterWriter.Init(sfFile, idxCommon.Plain, uint16(schema.PrimaryKey), sfIdx)
	if err != nil {
		return err
	}
	err = staticFilterWriter.AddValues(pkColumnData)
	if err != nil {
		return err
	}
	meta2, err := staticFilterWriter.Finalize()
	if err != nil {
		return err
	}
	metas.AddIndex(*meta2)
	metaBuf, err := metas.Marshal()
	if err != nil {
		return err
	}

	err = task.file.WriteIndexMeta(metaBuf)
	if err != nil {
		return err
	}
	if err = task.file.WriteBatch(task.data, task.ts); err != nil {
		return
	}
	return task.file.Sync()
}
