package frontend

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func handleExecUpgrade(ctx context.Context, ses *Session, st *tree.UpgradeStatement) error {
	retryCount := st.Retry
	if st.Retry <= 0 {
		retryCount = 1
	}
	err := ses.UpgradeTenant(ctx, st.Target.AccountName, uint32(retryCount), st.Target.IsALLAccount)
	if err != nil {
		return err
	}

	return nil
}
