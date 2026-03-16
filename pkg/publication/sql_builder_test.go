package publication

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateCcprSnapshotSQL_Default(t *testing.T) {
	sql := PublicationSQLBuilder.CreateCcprSnapshotSQL("snap1", "acc1", "pub1", "unknown", "db1", "t1")
	assert.Contains(t, sql, "snap1")
	assert.Contains(t, sql, "acc1")
	assert.Contains(t, sql, "pub1")
}

func TestGCStatusSQL(t *testing.T) {
	sql := PublicationSQLBuilder.GCStatusSQL()
	assert.NotEmpty(t, sql)
}

func TestUpdateMoCcprLogSQL(t *testing.T) {
	sql := PublicationSQLBuilder.UpdateMoCcprLogSQL("task1", 1, 100, "{}", "err", 2)
	assert.Contains(t, sql, "task1")
}
