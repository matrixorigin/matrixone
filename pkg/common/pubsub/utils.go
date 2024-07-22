package pubsub

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func InSubMetaTables(m *plan.SubscriptionMeta, tableName string) bool {
	if strings.ToLower(m.Tables) == TableAll {
		return true
	}

	for _, name := range strings.Split(m.Tables, Sep) {
		if name == tableName {
			return true
		}
	}
	return false
}

func AddSingleQuotesJoin(s []string) string {
	return "'" + strings.Join(s, "','") + "'"
}

func SplitAccounts(s string) (accounts []string) {
	for _, split := range strings.Split(s, Sep) {
		accounts = append(accounts, strings.TrimSpace(split))
	}
	return
}
