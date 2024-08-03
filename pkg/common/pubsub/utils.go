package pubsub

import (
	"slices"
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
	if len(s) == 0 {
		return ""
	}
	return "'" + strings.Join(s, "','") + "'"
}

func SplitAccounts(s string) (accounts []string) {
	if len(s) == 0 {
		return
	}
	for _, split := range strings.Split(s, Sep) {
		accounts = append(accounts, strings.TrimSpace(split))
	}
	return
}

func JoinAccounts(accountMap map[int32]*AccountInfo) string {
	accountNames := make([]string, 0, len(accountMap))
	for _, acc := range accountMap {
		accountNames = append(accountNames, acc.Name)
	}
	slices.Sort(accountNames)
	return strings.Join(accountNames, Sep)
}
