// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

func TestReverseBytes(t *testing.T) {
	tests := []struct {
		input    []byte
		expected []byte
	}{
		{[]byte("hello"), []byte("olleh")},
		{[]byte("world"), []byte("dlrow")},
		{[]byte("12345"), []byte("54321")},
		{[]byte(""), []byte("")},
	}

	for _, test := range tests {
		result := reverseBytes(test.input)
		if !bytes.Equal(result, test.expected) {
			t.Errorf("reverseBytes(%v) = %v; expected %v", test.input, result, test.expected)
		}
	}
}

func Test_GetUserPassword(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	bh := &backgroundExecTest{}
	bh.init()

	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	pu.SV.KillRountinesInterval = 0
	setPu("", pu)
	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
	rm, _ := NewRoutineManager(ctx, "")
	ses.rm = rm

	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses.SetTenantInfo(tenant)

	sql := getPasswordHistotyOfUserSql(rootName)
	mrs := newMrsForPasswordOfUser([][]interface{}{})
	bh.sql2result[sql] = mrs

	_, err := getUserPassword(ctx, bh, rootName)
	assert.NoError(t, err)
}

func Test_CheckPasswordHistoryRule(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	bh := &backgroundExecTest{}
	bh.init()

	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	pu.SV.KillRountinesInterval = 0
	setPu("", pu)
	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
	rm, _ := NewRoutineManager(ctx, "")
	ses.rm = rm

	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses.SetTenantInfo(tenant)

	reuseInfo := &passwordReuseInfo{
		PasswordHisoty:        int64(5),
		PasswordReuseInterval: int64(5),
	}

	userPasswords := []passwordHistoryRecord{}

	_, err := checkPasswordHistoryRule(ctx, reuseInfo, userPasswords, "123456")
	assert.NoError(t, err)
}

func Test_CheckPasswordIntervalRule(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	bh := &backgroundExecTest{}
	bh.init()

	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	pu.SV.KillRountinesInterval = 0
	setPu("", pu)
	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
	rm, _ := NewRoutineManager(ctx, "")
	ses.rm = rm

	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses.SetTenantInfo(tenant)

	reuseInfo := &passwordReuseInfo{
		PasswordHisoty:        int64(5),
		PasswordReuseInterval: int64(5),
	}

	userPasswords := []passwordHistoryRecord{}

	_, err := checkPasswordIntervalRule(ctx, reuseInfo, userPasswords, "123456")
	assert.NoError(t, err)
}

func Test_PasswordVerification(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	bh := &backgroundExecTest{}
	bh.init()

	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	pu.SV.KillRountinesInterval = 0
	setPu("", pu)
	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
	rm, _ := NewRoutineManager(ctx, "")
	ses.rm = rm

	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses.SetTenantInfo(tenant)

	reuseInfo := &passwordReuseInfo{
		PasswordHisoty:        int64(5),
		PasswordReuseInterval: int64(5),
	}

	userPasswords := []passwordHistoryRecord{}

	_, _, err := passwordVerification(ctx, reuseInfo, "123456", userPasswords)
	assert.NoError(t, err)
}

func TestCheckInvitedNodes(t *testing.T) {
	ctx := context.Background()

	// test empty invited nodes
	err := checkInvitedNodes(ctx, "")
	assert.Error(t, err)

	// test
	err = checkInvitedNodes(ctx, "192.168.1.1, 10.0.0.1")
	assert.NoError(t, err)

	// test invalid invited nodes
	err = checkInvitedNodes(ctx, "192.168.1.1, invalid_ip")
	assert.Error(t, err)

	// test CIDR
	err = checkInvitedNodes(ctx, "192.168.1.1, 10.0.0.0/33")
	assert.Error(t, err)

	// test CIDR
	err = checkInvitedNodes(ctx, "192.168.1.0/24")
	assert.NoError(t, err)

	// test "*"
	err = checkInvitedNodes(ctx, "*")
	assert.NoError(t, err)

	// test "*," should fail
	err = checkInvitedNodes(ctx, "192.168.1.1, *")
	assert.Error(t, err)
}

func TestCheckValidIpInInvitedNodes(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		invitedNodes string
		ip           string
		expectedErr  bool
	}{
		{
			invitedNodes: "192.168.1.0/24",
			ip:           "192.168.1.1",
			expectedErr:  false,
		},
		{
			invitedNodes: "192.168.1.0/24",
			ip:           "192.168.0.1",
			expectedErr:  true,
		},
		{
			invitedNodes: "192.168.0.1",
			ip:           "192.168.0.1",
			expectedErr:  false,
		},
		{
			invitedNodes: "192.168.0.1",
			ip:           "192.168.0.2",
			expectedErr:  true,
		},
		{
			invitedNodes: "*",
			ip:           "192.168.0.1",
			expectedErr:  false,
		},
		{
			invitedNodes: "192.168.0.1, 192.168.0.3",
			ip:           "192.168.0.3",
			expectedErr:  false,
		},
		{
			invitedNodes: "192.168.0.1, 192.168.0.3",
			ip:           "192.168.0.4",
			expectedErr:  true,
		},
		{
			invitedNodes: "",
			ip:           "127.0.0.1",
			expectedErr:  true,
		},
		{
			invitedNodes: "192.168.0.1, 192.168.0.3",
			ip:           "127.0.0.1",
			expectedErr:  false,
		},
		{
			invitedNodes: "192.168.0.1, 192.168.0.3",
			ip:           "",
			expectedErr:  true,
		},
	}
	for _, test := range tests {
		err := checkValidIpInInvitedNodes(ctx, test.invitedNodes, test.ip)
		if test.expectedErr {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestPasswordTimeIntervalCheck(t *testing.T) {
	tests := []struct {
		timeStr  string
		interval int64
		want     bool
	}{
		{"2024-11-11 06:45:56", 0, true},
		{"2024-11-11 06:46:01", 0, true},
		{"2024-11-11 06:48:30", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.timeStr, func(t *testing.T) {
			if got := passwordIntervalExpired(tt.timeStr, tt.interval); got != tt.want {
				t.Errorf("passwordTimeIntervalCheck(%v) = %v, want %v", tt.timeStr, got, tt.want)
			}
		})
	}
}

func Test_checkPasswordReusePolicy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	bh := &backgroundExecTest{}
	bh.init()

	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	pu.SV.KillRountinesInterval = 0
	setPu("", pu)
	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
	rm, _ := NewRoutineManager(ctx, "")
	ses.rm = rm

	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses.SetTenantInfo(tenant)

	ses.SetTenantInfo(tenant)
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

	user := "uarontestuser4"
	newPwd := "M@PasswordTestInTheFutur_129"

	ses.gSysVars.Set("password_reuse_interval", int64(2))
	ses.gSysVars.Set("password_history", int64(0))

	passwordRecord := []passwordHistoryRecord{
		{
			PasswordTimestamp: types.CurrentTimestamp().String2(time.UTC, 0),
			Password:          "M@PasswordTestInTheFutur_129",
		},
		{
			PasswordTimestamp: types.CurrentTimestamp().String2(time.UTC, 0),
			Password:          "M@PasswordTestInTheFutur_130",
		},
	}

	passwordHistory, err := json.Marshal(passwordRecord)
	assert.NoError(t, err)

	sql := getPasswordHistotyOfUserSql(user)
	mrs := newMrsForPasswordOfUser([][]interface{}{
		{passwordHistory},
	})
	bh.sql2result[sql] = mrs

	err = checkPasswordReusePolicy(ctx, ses, bh, newPwd, user)
	assert.Error(t, err)

	passwordRecord = []passwordHistoryRecord{
		{
			PasswordTimestamp: time.Now().AddDate(0, 0, -10).UTC().Format("2006-01-02 15:04:05"),
			Password:          "M@PasswordTestInTheFutur_129",
		},
		{
			PasswordTimestamp: time.Now().AddDate(0, 0, -10).UTC().Format("2006-01-02 15:04:05"),
			Password:          "M@PasswordTestInTheFutur_130",
		},
	}

	passwordHistory, err = json.Marshal(passwordRecord)
	assert.NoError(t, err)

	sql = getPasswordHistotyOfUserSql(user)
	mrs = newMrsForPasswordOfUser([][]interface{}{
		{passwordHistory},
	})
	bh.sql2result[sql] = mrs
	err = checkPasswordReusePolicy(ctx, ses, bh, newPwd, user)
	assert.NoError(t, err)
}
