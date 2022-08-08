package compile

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (s *Scope) CreateAccount(c *Compile) error {
	snapshot := engine.Snapshot(c.proc.Snapshot)
	accountName := s.Plan.GetAdmin().GetCreateAccount().GetName()
	accountCtx := context.WithValue(c.ctx, CreateAccount, accountName)
	//check the account exists or not

	return c.e.Create(accountCtx, accountName, snapshot)
}
