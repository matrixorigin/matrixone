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
	"time"
	"unicode"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	// ValidatePasswordCheckUserNameInPassword is the name of the global system variable
	ValidatePasswordCheckUserNameInPassword = "validate_password.check_user_name"
	ValidatePasswordVar                     = "validate_password"
	ValidatePasswordCheckCasePercentage     = "validate_password.changed_characters_percentage"
	ValidatePasswordPolicy                  = "validate_password.policy"
	ValidatePasswordLength                  = "validate_password.length"
	ValidatePasswordNumberCount             = "validate_password.number_count"
	ValidatePasswordSpecialCharCount        = "validate_password.special_char_count"
	ValidatePasswordMixedCount              = "validate_password.mixed_case_count"

	// password expiration management
	DefaultPasswordLifetime = "default_password_lifetime"

	// password history management
	PasswordHistory       = "password_history"
	PasswordReuseInterval = "password_reuse_interval"
	PasswordTimestamp     = "password_timestamp"
	Password              = "password"

	// password lock management
	ConnectionControlFailedConnectionsThreshold = "connection_control_failed_connections_threshold"
	ConnectionControlMaxConnectionDelay         = "connection_control_max_connection_delay"
)

type passwordHistoryRecord struct {
	PasswordTimestamp string `json:"password_timestamp"`
	Password          string `json:"password"`
}

type passwordReuseInfo struct {
	PasswordHisoty        int64
	PasswordReuseInterval int64
}

func needValidatePwd(session *Session) (bool, error) {
	value, err := session.GetGlobalSysVar(ValidatePasswordVar)
	if err != nil {
		return false, err
	}

	if value == nil {
		return false, nil
	}

	validatePasswordConfig, ok := value.(int8)
	if !ok || validatePasswordConfig != 1 {
		return false, nil
	}

	return true, nil
}

func validatePwd(ctx context.Context, pwd string, session *Session, authUserName, curUserName string) error {
	// check if the username is in the password
	err := validateCheckUserNameInPassword(ctx, pwd, authUserName, curUserName, session)
	if err != nil {
		return err
	}

	// check if the password contains enough changed characters
	err = validatePasswordCheckCasePercentage(ctx, pwd, session)
	if err != nil {
		return err
	}

	// check password policy
	err = validatePasswordPolicyInPassword(ctx, pwd, session)
	if err != nil {
		return err
	}
	return nil
}

// ValidateCheckUserNameInPassword check if the username is in the password
func validateCheckUserNameInPassword(ctx context.Context, pwd, authUserName, curUserName string, session *Session) error {
	value, err := session.GetGlobalSysVar(ValidatePasswordCheckUserNameInPassword)
	if err != nil {
		return err
	}

	if value == nil {
		return nil
	}

	checkUserNameConfig, ok := value.(int8)
	if !ok || checkUserNameConfig != 1 {
		return nil
	}

	pwdBytes := Slice(pwd)
	for _, userName := range []string{authUserName, curUserName} {
		userNameBytes := Slice(userName)
		if bytes.Equal(pwdBytes, userNameBytes) {
			return moerr.NewInvalidInputf(ctx, "Password '%s' euqals the user name '%s'", pwd, userName)
		} else if bytes.Equal(pwdBytes, reverseBytes(userNameBytes)) {
			return moerr.NewInvalidInputf(ctx, "Password '%s' euqals the reversed user name '%s'", pwd, userName)
		}
	}
	return nil
}

func reverseBytes(b []byte) []byte {
	reversed := make([]byte, len(b))
	for i := range b {
		reversed[i] = b[len(b)-1-i]
	}
	return reversed
}

// ValidatePasswordCheckCasePercentageInPassword check if the password contains enough changed characters
func validatePasswordCheckCasePercentage(ctx context.Context, password string, session *Session) error {
	value, err := session.GetGlobalSysVar(ValidatePasswordCheckCasePercentage)
	if err != nil {
		return err
	}

	if value == nil {
		return nil
	}

	percentageConfig, ok := value.(int64)
	if !ok || percentageConfig == 0 {
		return nil
	}

	changed := 0
	pwds := []rune(password)
	for i := 0; i < len(pwds); i++ {
		if unicode.IsUpper(pwds[i]) || unicode.IsLower(pwds[i]) {
			changed++
		}
	}

	if changed*100/len(password) < int(percentageConfig) {
		return moerr.NewInvalidInputf(ctx, "Password '%s' does not contain enough changed characters", password)
	}

	return nil
}

func validatePasswordPolicyInPassword(ctx context.Context, password string, session *Session) error {
	var (
		err          error
		value        interface{}
		policyConfig int64
		ok           bool
	)
	value, err = session.GetGlobalSysVar(ValidatePasswordPolicy)
	if err != nil {
		return err
	}

	if value == nil {
		return nil
	}

	policyConfig, ok = value.(int64)
	if !ok {
		return moerr.NewInvalidArg(ctx, "Invalid value for validate_password.policy", value)
	}

	if policyConfig == 0 {
		// low policy
		err = validatePasswordLengthInPassword(ctx, password, session)
		if err != nil {
			return err
		}
	} else {
		// medium policy
		err = validatePasswordLengthInPassword(ctx, password, session)
		if err != nil {
			return err
		}

		err = validatePassWordMediumPolicyInPassword(ctx, password, session)
		if err != nil {
			return err
		}
	}
	return err
}

func validatePasswordLengthInPassword(ctx context.Context, password string, session *Session) error {
	value, err := session.GetGlobalSysVar(ValidatePasswordLength)
	if err != nil {
		return err
	}

	if value == nil {
		return nil
	}

	lengthConfig, ok := value.(int64)
	if !ok {
		return moerr.NewInvalidInput(ctx, "Invalid value for validate_password.length")
	}

	if int64(len([]rune(password))) < lengthConfig {
		return moerr.NewInvalidInputf(ctx, "Password '%s' is too short, require at least %d characters", password, lengthConfig)
	}

	return nil
}

func validatePassWordMediumPolicyInPassword(ctx context.Context, password string, session *Session) error {

	var lowerCaseCnt, upperCaseCnt, numberCnt, specialCharCnt int64
	pwds := []rune(password)
	for i := 0; i < len(pwds); i++ {
		if unicode.IsUpper(pwds[i]) {
			upperCaseCnt++
		} else if unicode.IsLower(pwds[i]) {
			lowerCaseCnt++
		} else if unicode.IsDigit(pwds[i]) {
			numberCnt++
		} else {
			specialCharCnt++
		}
	}

	// mixed case count
	value, err := session.GetGlobalSysVar(ValidatePasswordMixedCount)
	if err != nil {
		return err
	}

	if value == nil {
		return nil
	}

	mixedCaseConfig, ok := value.(int64)
	if !ok {
		return moerr.NewInvalidArg(ctx, "Invalid value for validate_password.uppercase_count", value)
	}

	if lowerCaseCnt < mixedCaseConfig {
		return moerr.NewInvalidInputf(ctx, "Password '%s' does not meet the Lowercase requirements", password)
	} else if upperCaseCnt < mixedCaseConfig {
		return moerr.NewInvalidInputf(ctx, "Password '%s' does not meet the Uppercase requirements", password)
	}

	// number count
	value, err = session.GetGlobalSysVar(ValidatePasswordNumberCount)
	if err != nil {
		return err
	}

	if value == nil {
		return nil
	}

	numberConfig, ok := value.(int64)
	if !ok {
		return moerr.NewInvalidArg(ctx, "Invalid value for validate_password.number_count", value)
	}

	if numberConfig > 0 && numberCnt < numberConfig {
		return moerr.NewInvalidInputf(ctx, "Password '%s' does not meet the Number requirements", password)
	}

	// special char count
	value, err = session.GetGlobalSysVar(ValidatePasswordSpecialCharCount)
	if err != nil {
		return err
	}

	if value == nil {
		return nil
	}

	specialConfig, ok := value.(int64)
	if !ok {
		return moerr.NewInvalidArg(ctx, "Invalid value for validate_password.special_char_count", value)
	}

	if specialConfig > 0 && specialCharCnt < specialConfig {
		return moerr.NewInvalidInputf(ctx, "Password '%s' does not meet the Special Char requirements", password)
	}
	return nil
}

func getPasswordReuseInfo(session *Session) (*passwordReuseInfo, error) {
	passwordHistory, err := session.GetGlobalSysVar(PasswordHistory)
	if err != nil {
		return nil, err
	}

	passwordReuseInterval, err := session.GetGlobalSysVar(PasswordReuseInterval)
	if err != nil {
		return nil, err
	}

	return &passwordReuseInfo{
		PasswordHisoty:        passwordHistory.(int64),
		PasswordReuseInterval: passwordReuseInterval.(int64),
	}, nil
}

func whetherSavePasswordHistory(ses *Session) (bool, error) {
	// only when password_history and password_reuse_interval are set, the password history will be saved
	passwordReuseInfo, err := getPasswordReuseInfo(ses)
	if err != nil {
		return false, err
	}

	return passwordReuseInfo.PasswordHisoty > 0 && passwordReuseInfo.PasswordReuseInterval > 0, nil
}

func generateSinglePasswordRecod(pwd string) ([]byte, error) {
	records := make([]passwordHistoryRecord, 0)
	record := passwordHistoryRecord{
		PasswordTimestamp: types.CurrentTimestamp().String2(time.UTC, 0),
		Password:          pwd,
	}
	records = append(records, record)
	return json.Marshal(records)
}

func generageEmptyPasswordRecord() ([]byte, error) {
	records := make([]passwordHistoryRecord, 0)
	return json.Marshal(records)
}

func getUserPassword(ctx context.Context, bh BackgroundExec, user string) ([]passwordHistoryRecord, error) {
	var (
		err             error
		sql             string
		erArray         []ExecResult
		passowrdHistory string
	)
	// get the number of password records for the current user
	sql = getPasswordHistotyOfUserSql(user)

	bh.ClearExecResultSet()
	bh.Exec(ctx, sql)

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	var records []passwordHistoryRecord
	if execResultArrayHasData(erArray) {
		passowrdHistory, err = erArray[0].GetString(ctx, 0, 0)
		if err != nil {
			return nil, err
		}

		// parse the password history to get the number of password records
		err = json.Unmarshal([]byte(passowrdHistory), &records)
		if err != nil {
			return nil, err
		}

		return records, nil
	}

	return records, nil
}

func checkPasswordHistoryRule(ctx context.Context, reuseInfo *passwordReuseInfo, userPasswords []passwordHistoryRecord, pwd string) (canUse bool, err error) {
	if len(userPasswords) == 0 {
		return true, nil
	}

	// check the password history
	// from the latest password record to the oldest password record
	// check time not exceed the password history
	checkNum := reuseInfo.PasswordHisoty - 1
	for i := len(userPasswords) - 1; i >= 0 && checkNum >= 0; i-- {
		if userPasswords[i].Password == pwd {
			return false, moerr.NewInvalidInputf(ctx, "The password has been used before, please change another one.")
		}
		checkNum--
	}

	return true, nil
}

func checkPasswordIntervalRule(ctx context.Context, reuseInfo *passwordReuseInfo, userPasswords []passwordHistoryRecord, pwd string) (canUse bool, err error) {
	if len(userPasswords) == 0 {
		return true, nil
	}

	// check the password interval
	for _, record := range userPasswords {
		if record.Password == pwd {
			// check the password reuse interval
			var passwordTime time.Time
			passwordTime, err = time.ParseInLocation("2006-01-02 15:04:05", record.PasswordTimestamp, time.UTC)
			if err != nil {
				return false, err
			}

			if passwordTime.AddDate(0, 0, int(reuseInfo.PasswordReuseInterval)).After(time.Now()) {
				return false, moerr.NewInvalidInputf(ctx, "The password has been used before, please change another one")
			}
		}
	}
	return true, nil
}

func passwordVerification(ctx context.Context, reuseInfo *passwordReuseInfo, pwd string, userPasswords []passwordHistoryRecord) (bool, int64, error) {
	var (
		err             error
		canUse          bool
		userPasswordNum int64
	)

	// getUserPasswordNum
	if len(userPasswords) == 0 {
		return true, 0, nil
	}
	userPasswordNum = int64(len(userPasswords))

	canDeleteNum := userPasswordNum - reuseInfo.PasswordHisoty + 1
	if canDeleteNum < 0 {
		canDeleteNum = 0
	}

	// check the password history and password interval
	if reuseInfo.PasswordHisoty > 0 {
		// check the password history
		canUse, err = checkPasswordHistoryRule(ctx, reuseInfo, userPasswords, pwd)
		if err != nil {
			return false, 0, err
		}

		if !canUse {
			return false, 0, moerr.NewInvalidInputf(ctx, "The password has been used before, please change another one")
		}
	}

	if reuseInfo.PasswordReuseInterval > 0 {
		// check the password interval
		canUse, err = checkPasswordIntervalRule(ctx, reuseInfo, userPasswords, pwd)
		if err != nil {
			return false, 0, err
		}

		if !canUse {
			return false, 0, moerr.NewInvalidInputf(ctx, "The password has been used before, please change another one")
		}

	}

	return true, canDeleteNum, nil
}

func checkPasswordReusePolicy(ctx context.Context, ses *Session, bh BackgroundExec, pwd string, user string) error {
	var (
		err           error
		canUse        bool
		canDeleteNum  int64
		reuseInfo     *passwordReuseInfo
		userPasswords []passwordHistoryRecord
		sql           string
	)

	// if user is super user, no need to check the password reuse policy
	if isSuperUser(user) {
		return nil
	}
	// get the password reuse information
	reuseInfo, err = getPasswordReuseInfo(ses)
	if err != nil {
		return err
	}

	if reuseInfo.PasswordHisoty <= 0 || reuseInfo.PasswordReuseInterval <= 0 {
		return nil
	}

	// get the password history
	userPasswords, err = getUserPassword(ctx, bh, user)
	if err != nil {
		return err
	}

	// check the password reuse policy
	canUse, canDeleteNum, err = passwordVerification(ctx, reuseInfo, pwd, userPasswords)
	if err != nil {
		return err
	}
	if !canUse {
		return moerr.NewInvalidInputf(ctx, "The password has been used before, please change another one")
	}

	// delete the password records that exceed the password history
	deleteNum := 0
	if canDeleteNum > 0 {
		for i := 0; i < int(canDeleteNum); i++ {
			// if password time exceeds the password history, delete the password record
			var passwordTime time.Time
			passwordTime, err = time.ParseInLocation("2006-01-02 15:04:05", userPasswords[i].PasswordTimestamp, time.UTC)
			if err != nil {
				return err
			}

			if passwordTime.AddDate(0, 0, int(reuseInfo.PasswordHisoty)).Before(time.Now()) {
				deleteNum++
			} else {
				break
			}
		}

		if deleteNum > 0 {
			userPasswords = userPasswords[deleteNum:]
		}
	}

	// add the new password record
	newRecord := passwordHistoryRecord{
		PasswordTimestamp: types.CurrentTimestamp().String2(time.UTC, 0),
		Password:          pwd,
	}

	userPasswords = append(userPasswords, newRecord)

	// save the password history
	var passwordHistory []byte
	passwordHistory, err = json.Marshal(userPasswords)
	if err != nil {
		return err
	}

	// update the password history
	sql = getSqlForUpdatePasswordHistoryOfUser(string(passwordHistory), user)

	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}

	return nil
}
