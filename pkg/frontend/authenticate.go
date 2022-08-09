package frontend

import (
	"fmt"
	"strings"
)

type TenantInfo struct {
	Tenant      string
	User        string
	DefaultRole string
}

func (ti *TenantInfo) String() string {
	return fmt.Sprintf("%s:%s:%s", ti.Tenant, ti.User, ti.DefaultRole)
}

func (ti *TenantInfo) GetTenant() string {
	return ti.Tenant
}

func (ti *TenantInfo) GetUser() string {
	return ti.User
}

func (ti *TenantInfo) GetDefaultRole() string {
	return ti.DefaultRole
}

func GetDefaultTenant() string {
	return "sys"
}

func GetDefaultRole() string {
	return "public"
}

/**
GetTenantInfo extract tenant info from the input of the user.

The format of the user
1. tenant:user:role
2. tenant:user
3. user
*/
func GetTenantInfo(userInput string) (*TenantInfo, error) {
	p := strings.IndexByte(userInput, ':')
	if p == -1 {
		return &TenantInfo{
			Tenant:      GetDefaultTenant(),
			User:        userInput,
			DefaultRole: GetDefaultRole(),
		}, nil
	} else {
		tenant := userInput[:p]
		tenant = strings.TrimSpace(tenant)
		if len(tenant) == 0 {
			return &TenantInfo{}, fmt.Errorf("invalid tenant name '%s'", tenant)
		}
		userRole := userInput[p+1:]
		p2 := strings.IndexByte(userRole, ':')
		if p2 == -1 {
			//tenant:user
			user := userRole
			user = strings.TrimSpace(user)
			if len(user) == 0 {
				return &TenantInfo{}, fmt.Errorf("invalid user name '%s'", user)
			}
			return &TenantInfo{
				Tenant:      tenant,
				User:        user,
				DefaultRole: GetDefaultRole(),
			}, nil
		} else {
			user := userRole[:p2]
			user = strings.TrimSpace(user)
			if len(user) == 0 {
				return &TenantInfo{}, fmt.Errorf("invalid user name '%s'", user)
			}
			role := userRole[p2+1:]
			role = strings.TrimSpace(role)
			if len(role) == 0 {
				return &TenantInfo{}, fmt.Errorf("invalid role name '%s'", role)
			}
			return &TenantInfo{
				Tenant:      tenant,
				User:        user,
				DefaultRole: role,
			}, nil
		}
	}
}
