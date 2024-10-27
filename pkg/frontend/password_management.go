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
	"unicode"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
)

func needValidatePassword(session *Session) (bool, error) {
	value, err := session.GetGlobalSysVar(ValidatePasswordVar)
	if err != nil {
		return false, err
	}

	if value == nil {
		return false, nil
	}

	validatePassword, ok := value.(int8)
	if !ok || validatePassword != 1 {
		return false, nil
	}

	return true, nil
}

func validatePassword(ctx context.Context, pwd string, session *Session, authUserName, curUserName string) error {
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

	checkUserName, ok := value.(int8)
	if !ok || checkUserName != 1 {
		return nil
	}

	pwdBytes := Slice(pwd)
	for _, userName := range []string{authUserName, curUserName} {
		userNameBytes := Slice(userName)
		if bytes.Contains(pwdBytes, userNameBytes) {
			return moerr.NewInvalidInputf(ctx, "Password '%s' contains the user name '%s'", pwd, userName)
		} else if bytes.Contains(pwdBytes, reverseBytes(userNameBytes)) {
			return moerr.NewInvalidInputf(ctx, "Password '%s' contains the reversed user name '%s'", pwd, userName)
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

	percentage, ok := value.(int64)
	if !ok || percentage == 0 {
		return nil
	}

	changed := 0
	pwds := []rune(password)
	for i := 0; i < len(pwds); i++ {
		if unicode.IsUpper(pwds[i]) || unicode.IsLower(pwds[i]) {
			changed++
		}
	}

	if changed*100/len(password) < int(percentage) {
		return moerr.NewInvalidInputf(ctx, "Password '%s' does not contain enough changed characters", password)
	}

	return nil
}

func validatePasswordPolicyInPassword(ctx context.Context, password string, session *Session) error {
	value, err := session.GetGlobalSysVar(ValidatePasswordPolicy)
	if err != nil {
		return err
	}

	if value == nil {
		return nil
	}

	policy, ok := value.(int64)
	if !ok {
		return moerr.NewInvalidArg(ctx, "Invalid value for validate_password.policy", value)
	}

	if policy == 0 {
		// low policy
		err := validatePasswordLengthInPassword(ctx, password, session)
		if err != nil {
			return err
		}
	} else {
		// medium policy
		err := validatePasswordLengthInPassword(ctx, password, session)
		if err != nil {
			return err
		}

		err = validatePassWordMediumPolicyInPassword(ctx, password, session)
		if err != nil {
			return err
		}
	}
	return nil
}

func validatePasswordLengthInPassword(ctx context.Context, password string, session *Session) error {
	value, err := session.GetGlobalSysVar(ValidatePasswordLength)
	if err != nil {
		return err
	}

	if value == nil {
		return nil
	}

	length, ok := value.(int64)
	if !ok {
		return moerr.NewInvalidInput(ctx, "Invalid value for validate_password.length")
	}

	if int64(len([]rune(password))) < length {
		return moerr.NewInvalidInputf(ctx, "Password '%s' is too short, require at least %d characters", password, length)
	}

	return nil
}

func validatePassWordMediumPolicyInPassword(ctx context.Context, password string, session *Session) error {

	var lowerCaseCount, upperCaseCount, numberCount, specialCharCount int64
	pwds := []rune(password)
	for i := 0; i < len(pwds); i++ {
		if unicode.IsUpper(pwds[i]) {
			upperCaseCount++
		} else if unicode.IsLower(pwds[i]) {
			lowerCaseCount++
		} else if unicode.IsDigit(pwds[i]) {
			numberCount++
		} else {
			specialCharCount++
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

	mixedCase, ok := value.(int64)
	if !ok {
		return moerr.NewInvalidArg(ctx, "Invalid value for validate_password.uppercase_count", value)
	}

	if lowerCaseCount < mixedCase {
		return moerr.NewInvalidInputf(ctx, "Password '%s' does not meet the Lowercase requirements", password)
	} else if upperCaseCount < mixedCase {
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

	number, ok := value.(int64)
	if !ok {
		return moerr.NewInvalidArg(ctx, "Invalid value for validate_password.number_count", value)
	}

	if number > 0 && numberCount < number {
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

	special, ok := value.(int64)
	if !ok {
		return moerr.NewInvalidArg(ctx, "Invalid value for validate_password.special_char_count", value)
	}

	if special > 0 && specialCharCount < special {
		return moerr.NewInvalidInputf(ctx, "Password '%s' does not meet the Special Char requirements", password)
	}
	return nil
}
