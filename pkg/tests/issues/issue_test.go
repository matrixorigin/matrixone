package issues

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestWWConflict(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			cn2, err := c.GetCNService(1)
			require.NoError(t, err)

			db := getDatabaseName(t)
			table := "t"

			createTableAndWaitCNApplied(
				t,
				db,
				table,
				"create table "+table+" (id int primary key, v int)",
				cn1,
				cn2,
			)

			committedAt := execSQL(
				t,
				db,
				"insert into "+table+" values (1, 1)",
				cn1,
			)

			// workflow:
			// cn1: txn1 update t
			// cn1: txn1 lock mo_tables, and found changed in lock op
			// cn2: start txn2 update
			// cn2: txn2 updated
			// cn1: retry lock mo_tables and continue update
			// no ww conflict error

			var wg sync.WaitGroup
			wg.Add(2)

			txn2StartedC := make(chan struct{})
			txn2CommittedC := make(chan struct{})

			// txn1
			go func() {
				defer wg.Done()

				exec1 := getSQLExecutor(cn1)
				err := exec1.ExecTxn(
					ctx,
					func(txn executor.TxnExecutor) error {
						tx1 := txn.Txn().Txn().ID

						runtime.MustGetTestingContext(cn1.ServiceID()).SetAdjustCheckDataChangedAfterLocked(
							func(
								txnID []byte,
								tableID uint64,
								changed bool,
							) bool {
								if !bytes.Equal(txnID, tx1) {
									return changed
								}

								if tableID != catalog.MO_TABLES_ID {
									return changed
								}

								close(txn2StartedC)
								// wait txn2 update committed
								<-txn2CommittedC
								return true
							},
						)

						// update t set v = 2 where id = 1
						res, err := txn.Exec(
							"update "+table+" set v = 1 where id = 1",
							executor.StatementOption{},
						)
						if err != nil {
							return err
						}
						res.Close()

						return nil
					},
					executor.Options{}.
						WithDatabase(db).
						WithMinCommittedTS(committedAt),
				)
				require.NoError(t, err)
			}()

			go func() {
				defer func() {
					close(txn2CommittedC)
					wg.Done()
				}()

				<-txn2StartedC
				exec := getSQLExecutor(cn2)

				res, err := exec.Exec(
					ctx,
					"update "+table+" set v = 2 where id = 1",
					executor.Options{}.
						WithDatabase(db).
						WithMinCommittedTS(committedAt),
				)
				require.NoError(t, err)
				res.Close()
			}()

			wg.Wait()
		},
	)
}
