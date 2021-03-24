package variable

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ScopeFlag is for system variable whether can be changed in global/session dynamically or not.
type ScopeFlag uint8

// TypeFlag is the SysVar type, which doesn't exactly match MySQL types.
type TypeFlag byte

const (
	// ScopeNone means the system variable can not be changed dynamically.
	ScopeNone ScopeFlag = 0
	// ScopeGlobal means the system variable can be changed globally.
	ScopeGlobal ScopeFlag = 1 << 0
	// ScopeSession means the system variable can only be changed in current session.
	ScopeSession ScopeFlag = 1 << 1

	// TypeStr is the default
	TypeStr TypeFlag = 0
	// TypeBool for boolean
	TypeBool TypeFlag = 1
	// TypeInt for integer
	TypeInt TypeFlag = 2
	// TypeEnum for Enum
	TypeEnum TypeFlag = 3
	// TypeFloat for Double
	TypeFloat TypeFlag = 4
	// TypeUnsigned for Unsigned integer
	TypeUnsigned TypeFlag = 5
	// TypeTime for time of day (a TiDB extension)
	TypeTime TypeFlag = 6
	// TypeDuration for a golang duration (a TiDB extension)
	TypeDuration TypeFlag = 7

	// BoolOff is the canonical string representation of a boolean false.
	BoolOff = "OFF"
	// BoolOn is the canonical string representation of a boolean true.
	BoolOn = "ON"
	// On is the canonical string for ON
	On = "ON"

	// Off is the canonical string for OFF
	Off = "OFF"

	// Warn means return warnings
	Warn = "WARN"
)

// SysVar is for system variable.
type SysVar struct {
	// Scope is for whether can be changed or not
	Scope ScopeFlag
	// Name is the variable name.
	Name string
	// Value is the variable value.
	Value string
	// Type is the MySQL type (optional)
	Type TypeFlag
	// MinValue will automatically be validated when specified (optional)
	MinValue int64
	// MaxValue will automatically be validated when specified (optional)
	MaxValue uint64
	// AutoConvertNegativeBool applies to boolean types (optional)
	AutoConvertNegativeBool bool
	// AutoConvertOutOfRange applies to int and unsigned types.
	AutoConvertOutOfRange bool
	// ReadOnly applies to all types
	ReadOnly bool
	// PossibleValues applies to ENUM type
	PossibleValues []string
	// AllowEmpty is a special TiDB behavior which means "read value from config" (do not use)
	AllowEmpty bool
	// AllowEmptyAll is a special behavior that only applies to TiDBCapturePlanBaseline, TiDBTxnMode (do not use)
	AllowEmptyAll bool
	// AllowAutoValue means that the special value "-1" is permitted, even when outside of range.
	AllowAutoValue bool
	// Validation is a callback after the type validation has been performed
	Validation func(*SessionVars, string, string, ScopeFlag) (string, error)
	// IsHintUpdatable indicate whether it's updatable via SET_VAR() hint (optional)
	IsHintUpdatable bool
}

// ValidateFromType provides automatic validation based on the SysVar's type
func (sv *SysVar) ValidateFromType(vars *SessionVars, value string, scope ScopeFlag) (string, error) {
	// Some sysvars are read-only. Attempting to set should always fail.
	if sv.ReadOnly || sv.Scope == ScopeNone {
		return value, ErrIncorrectScope.GenWithStackByArgs(sv.Name, "read only")
	}
	// The string "DEFAULT" is a special keyword in MySQL, which restores
	// the compiled sysvar value. In which case we can skip further validation.
	if strings.EqualFold(value, "DEFAULT") {
		return sv.Value, nil
	}
	// Some sysvars in TiDB have a special behavior where the empty string means
	// "use the config file value". This needs to be cleaned up once the behavior
	// for instance variables is determined.
	if value == "" && ((sv.AllowEmpty && scope == ScopeSession) || sv.AllowEmptyAll) {
		return value, nil
	}
	// Provide validation using the SysVar struct
	switch sv.Type {
	case TypeUnsigned:
		return sv.checkUInt64SystemVar(value, vars)
	case TypeInt:
		return sv.checkInt64SystemVar(value, vars)
	case TypeBool:
		return sv.checkBoolSystemVar(value, vars)
	case TypeFloat:
		return sv.checkFloatSystemVar(value, vars)
	case TypeEnum:
		return sv.checkEnumSystemVar(value, vars)
	case TypeTime:
		return sv.checkTimeSystemVar(value, vars)
	case TypeDuration:
		return sv.checkDurationSystemVar(value, vars)
	}
	return value, nil // typeString
}

const (
	localDayTimeFormat = "15:04"
	// FullDayTimeFormat is the full format of analyze start time and end time.
	FullDayTimeFormat = "15:04 -0700"
)

func (sv *SysVar) checkTimeSystemVar(value string, vars *SessionVars) (string, error) {
	var t time.Time
	var err error
	if len(value) <= len(localDayTimeFormat) {
		t, err = time.ParseInLocation(localDayTimeFormat, value, vars.TimeZone)
	} else {
		t, err = time.ParseInLocation(FullDayTimeFormat, value, vars.TimeZone)
	}
	if err != nil {
		return "", err
	}
	return t.Format(FullDayTimeFormat), nil
}

func (sv *SysVar) checkDurationSystemVar(value string, vars *SessionVars) (string, error) {
	d, err := time.ParseDuration(value)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	// Check for min/max violations
	if int64(d) < sv.MinValue {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if uint64(d) > sv.MaxValue {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	// return a string representation of the duration
	return d.String(), nil
}

func (sv *SysVar) checkUInt64SystemVar(value string, vars *SessionVars) (string, error) {
	if sv.AllowAutoValue && value == "-1" {
		return value, nil
	}
	// There are two types of validation behaviors for integer values. The default
	// is to return an error saying the value is out of range. For MySQL compatibility, some
	// values prefer convert the value to the min/max and return a warning.
	if !sv.AutoConvertOutOfRange {
		return sv.checkUint64SystemVarWithError(value)
	}
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if value[0] == '-' {
		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
		}
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MinValue), nil
	}
	val, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < uint64(sv.MinValue) {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MinValue), nil
	}
	if val > sv.MaxValue {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MaxValue), nil
	}
	return value, nil
}

func (sv *SysVar) checkInt64SystemVar(value string, vars *SessionVars) (string, error) {
	if sv.AllowAutoValue && value == "-1" {
		return value, nil
	}
	// There are two types of validation behaviors for integer values. The default
	// is to return an error saying the value is out of range. For MySQL compatibility, some
	// values prefer convert the value to the min/max and return a warning.
	if !sv.AutoConvertOutOfRange {
		return sv.checkInt64SystemVarWithError(value)
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < sv.MinValue {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MinValue), nil
	}
	if val > int64(sv.MaxValue) {
		vars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(sv.Name, value))
		return fmt.Sprintf("%d", sv.MaxValue), nil
	}
	return value, nil
}

func (sv *SysVar) checkEnumSystemVar(value string, vars *SessionVars) (string, error) {
	// The value could be either a string or the ordinal position in the PossibleValues.
	// This allows for the behavior 0 = OFF, 1 = ON, 2 = DEMAND etc.
	var iStr string
	for i, v := range sv.PossibleValues {
		iStr = fmt.Sprintf("%d", i)
		if strings.EqualFold(value, v) || strings.EqualFold(value, iStr) {
			return v, nil
		}
	}
	return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
}

func (sv *SysVar) checkFloatSystemVar(value string, vars *SessionVars) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < float64(sv.MinValue) || val > float64(sv.MaxValue) {
		return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
	}
	return value, nil
}

func (sv *SysVar) checkBoolSystemVar(value string, vars *SessionVars) (string, error) {
	if strings.EqualFold(value, "ON") {
		return BoolOn, nil
	} else if strings.EqualFold(value, "OFF") {
		return BoolOff, nil
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		// There are two types of conversion rules for integer values.
		// The default only allows 0 || 1, but a subset of values convert any
		// negative integer to 1.
		if !sv.AutoConvertNegativeBool {
			if val == 0 {
				return BoolOff, nil
			} else if val == 1 {
				return BoolOn, nil
			}
		} else {
			if val == 1 || val < 0 {
				return BoolOn, nil
			} else if val == 0 {
				return BoolOff, nil
			}
		}
	}
	return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
}

func (sv *SysVar) checkUint64SystemVarWithError(value string) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if value[0] == '-' {
		// // in strict it expects the error WrongValue, but in non-strict it returns WrongType
		return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
	}
	val, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < uint64(sv.MinValue) || val > sv.MaxValue {
		return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
	}
	return value, nil
}

func (sv *SysVar) checkInt64SystemVarWithError(value string) (string, error) {
	if len(value) == 0 {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(sv.Name)
	}
	if val < sv.MinValue || val > int64(sv.MaxValue) {
		return value, ErrWrongValueForVar.GenWithStackByArgs(sv.Name, value)
	}
	return value, nil
}

var sysVars map[string]*SysVar
var sysVarsLock sync.RWMutex

// RegisterSysVar adds a sysvar to the SysVars list
func RegisterSysVar(sv *SysVar) {
	name := strings.ToLower(sv.Name)
	sysVarsLock.Lock()
	sysVars[name] = sv
	sysVarsLock.Unlock()
}

// UnregisterSysVar removes a sysvar from the SysVars list
// currently only used in tests.
func UnregisterSysVar(name string) {
	name = strings.ToLower(name)
	sysVarsLock.Lock()
	delete(sysVars, name)
	sysVarsLock.Unlock()
}

// GetSysVar returns sys var info for name as key.
func GetSysVar(name string) *SysVar {
	name = strings.ToLower(name)
	sysVarsLock.RLock()
	defer sysVarsLock.RUnlock()
	return sysVars[name]
}

// SetSysVar sets a sysvar. This will not propagate to the cluster, so it should only be
// used for instance scoped AUTO variables such as system_time_zone.
func SetSysVar(name string, value string) {
	name = strings.ToLower(name)
	sysVarsLock.Lock()
	defer sysVarsLock.Unlock()
	sysVars[name].Value = value
}

// GetSysVars returns the sysVars list under a RWLock
func GetSysVars() map[string]*SysVar {
	sysVarsLock.RLock()
	defer sysVarsLock.RUnlock()
	return sysVars
}

// ValidateFromHook calls the anonymous function on the sysvar if it exists.
func (sv *SysVar) ValidateFromHook(vars *SessionVars, normalizedValue string, originalValue string, scope ScopeFlag) (string, error) {
	if sv.Validation != nil {
		return sv.Validation(vars, normalizedValue, originalValue, scope)
	}
	return normalizedValue, nil
}

// SetNamesVariables is the system variable names related to set names statements.
var SetNamesVariables = []string{
	CharacterSetClient,
	CharacterSetConnection,
	CharacterSetResults,
}

// SetCharsetVariables is the system variable names related to set charset statements.
var SetCharsetVariables = []string{
	CharacterSetClient,
	CharacterSetResults,
}

const (
	// CharacterSetConnection is the name for character_set_connection system variable.
	CharacterSetConnection = "character_set_connection"
	// CollationConnection is the name for collation_connection system variable.
	CollationConnection = "collation_connection"
	// CharsetDatabase is the name for character_set_database system variable.
	CharsetDatabase = "character_set_database"
	// CollationDatabase is the name for collation_database system variable.
	CollationDatabase = "collation_database"
	// CharacterSetFilesystem is the name for character_set_filesystem system variable.
	CharacterSetFilesystem = "character_set_filesystem"
	// CharacterSetClient is the name for character_set_client system variable.
	CharacterSetClient = "character_set_client"
	// CharacterSetSystem is the name for character_set_system system variable.
	CharacterSetSystem = "character_set_system"
	// CharacterSetServer is the name of 'character_set_server' system variable.
	CharacterSetServer = "character_set_server"
	// AutoCommit is the name for 'autocommit' system variable.
	AutoCommit = "autocommit"
	// AutoIncrementIncrement is the name of 'auto_increment_increment' system variable.
	AutoIncrementIncrement = "auto_increment_increment"
	// AutoIncrementOffset is the name of 'auto_increment_offset' system variable.
	AutoIncrementOffset = "auto_increment_offset"
	// CollationServer is the name of 'collation_server' variable.
	CollationServer = "collation_server"
	// WarningCount is the name for 'warning_count' system variable.
	WarningCount = "warning_count"
	// ErrorCount is the name for 'error_count' system variable.
	ErrorCount = "error_count"
	// SQLSelectLimit is the name for 'sql_select_limit' system variable.
	SQLSelectLimit = "sql_select_limit"
)

// GlobalVarAccessor is the interface for accessing global scope system and status variables.
type GlobalVarAccessor interface {
	// GetGlobalSysVar gets the global system variable value for name.
	GetGlobalSysVar(name string) (string, error)
	// SetGlobalSysVar sets the global system variable name to value.
	SetGlobalSysVar(name string, value string) error
}
