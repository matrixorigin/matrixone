package frontend

import (
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_verifyAccountCanOperateClusterTable(t *testing.T) {
	type arg struct {
		acc  *TenantInfo
		db   string
		op   clusterTableOperationType
		want bool
	}

	sys := &TenantInfo{
		Tenant: sysAccountName,
	}

	nonSys := &TenantInfo{
		Tenant: "abc",
	}

	var args []arg

	for db := range bannedCatalogDatabases {
		for i := clusterTableNone; i <= clusterTableDrop; i++ {
			args = append(args, arg{
				acc:  sys,
				db:   db,
				op:   i,
				want: db == moCatalog,
			})
			args = append(args, arg{
				acc:  sys,
				db:   "abc",
				op:   i,
				want: false,
			})
			args = append(args, arg{
				acc:  nonSys,
				db:   db,
				op:   i,
				want: db == moCatalog && (i == clusterTableNone || i == clusterTableSelect),
			})
			args = append(args, arg{
				acc:  nonSys,
				db:   "abc",
				op:   i,
				want: false,
			})
		}
	}

	for _, a := range args {
		ret := verifyAccountCanOperateClusterTable(a.acc, a.db, a.op)
		assert.True(t, ret == a.want)
	}
}

func Test_verifyLightPrivilege(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	type arg struct {
		realUser         bool
		db               string
		directDb         bool
		clusterTable     bool
		clusterOperation clusterTableOperationType
	}

	ses := newTestSession(t, ctrl)
	defer ses.Dispose()

	sys := &TenantInfo{
		Tenant: sysAccountName,
	}

	nonSys := &TenantInfo{
		Tenant: "abc",
	}

	ses.SetFromRealUser(true)
	ses.SetTenantInfo(sys)

	var ret bool

	ret = verifyLightPrivilege(ses, moCatalog, true,
		false, clusterTableNone)
	assert.False(t, ret)

	ret = verifyLightPrivilege(ses, moCatalog, true,
		true, clusterTableCreate)
	assert.True(t, ret)

	ret = verifyLightPrivilege(ses, "abc", true,
		true, clusterTableCreate)
	assert.False(t, ret)

	ret = verifyLightPrivilege(ses, "abc", true,
		false, clusterTableCreate)
	assert.True(t, ret)

	ret = verifyLightPrivilege(ses, "abc", false,
		false, clusterTableCreate)
	assert.True(t, ret)

	ses.SetTenantInfo(nonSys)

	ret = verifyLightPrivilege(ses, moCatalog, true,
		false, clusterTableNone)
	assert.False(t, ret)

	ret = verifyLightPrivilege(ses, moCatalog, true,
		true, clusterTableCreate)
	assert.False(t, ret)

	ret = verifyLightPrivilege(ses, moCatalog, true,
		true, clusterTableSelect)
	assert.True(t, ret)

	ret = verifyLightPrivilege(ses, moCatalog, true,
		true, clusterTableNone)
	assert.True(t, ret)

	ret = verifyLightPrivilege(ses, "abc", true,
		true, clusterTableCreate)
	assert.False(t, ret)

	ret = verifyLightPrivilege(ses, "abc", true,
		false, clusterTableCreate)
	assert.True(t, ret)

	ret = verifyLightPrivilege(ses, "abc", false,
		false, clusterTableCreate)
	assert.True(t, ret)
}
