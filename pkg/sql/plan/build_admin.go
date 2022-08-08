package plan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildCreateAccount(stmt *tree.CreateAccount, ctx CompilerContext) (*Plan, error) {
	account := &plan.CreateAccount{
		IfNotExists: stmt.IfNotExists,
		Name:        stmt.Name,
		AuthOption: &plan.AccountAuthOption{
			AdminName: stmt.AuthOption.AdminName,
			IdentifiedType: &plan.AccountIdentified{
				Str: stmt.AuthOption.IdentifiedType.Str,
			},
		},
	}

	switch stmt.AuthOption.IdentifiedType.Typ {
	case tree.AccountIdentifiedByPassword:
		account.AuthOption.IdentifiedType.Typ = plan.AccountIdentified_ACCOUNT_IDENTIFIED_BY_PASSWORD
	case tree.AccountIdentifiedByRandomPassword:
		account.AuthOption.IdentifiedType.Typ = plan.AccountIdentified_ACCOUNT_IDENTIFIED_BY_RANDOM_PASSWORD
	case tree.AccountIdentifiedWithSSL:
		account.AuthOption.IdentifiedType.Typ = plan.AccountIdentified_ACCOUNT_IDENTIFIED_WITH_SSL
	}

	if stmt.StatusOption.Exist {
		account.StatusOption = &plan.AccountStatus{}
		switch stmt.StatusOption.Option {
		case tree.AccountStatusOpen:
			account.StatusOption.Option = plan.AccountStatus_ACCOUNT_STATUS_OPEN
		case tree.AccountStatusSuspend:
			account.StatusOption.Option = plan.AccountStatus_ACCOUNT_STATUS_SUSPEND
		}
	}

	if stmt.Comment.Exist {
		account.AccountComment = &plan.AccountComment{Comment: stmt.Comment.Comment}
	}

	accountPlan := &Plan{
		Plan: &plan.Plan_Admin{
			Admin: &plan.Administration{
				AdminType: plan.Administration_CREATE_ACCOUNT,
				Definition: &plan.Administration_CreateAccount{
					CreateAccount: account,
				},
			},
		},
	}
	return accountPlan, nil
}
