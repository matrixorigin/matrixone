// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Validate new requests, not historical sysvar/catalog decoding. In particular,
// retaining a legacy snapshot does not grant permission to declare latin1 anew.
func validateCharsetSystemVariable(name string, value interface{}) error {
	charsetVar := strings.HasPrefix(name, "character_set_")
	collationVar := strings.HasPrefix(name, "collation_") || name == "default_collation_for_utf8mb4"
	if !charsetVar && !collationVar {
		return nil
	}
	text, ok := value.(string)
	if !ok {
		return moerr.NewInvalidInputNoCtx("character set/collation must be a string")
	}
	// Preserve the existing connection default sentinel; it is not a native
	// SQL collation spelling and must not be added to the collation registry.
	if name == "collation_connection" && strings.EqualFold(text, "default") {
		return nil
	}
	// NULL character_set_results disables result conversion. String variable
	// conversion historically represents that NULL as the empty string.
	if name == "character_set_results" && text == "" {
		return nil
	}
	if charsetVar {
		if _, ok := collation.ResolveCharset(text); !ok {
			return moerr.NewInvalidInputNoCtxf("unsupported character set '%s'", text)
		}
	} else if _, _, ok := collation.ResolveSession(text); !ok {
		return moerr.NewInvalidInputNoCtxf("unsupported collation '%s'", text)
	}
	return nil
}

// Check SQL charset syntax before the existing SET execution mutates anything.
// Do not classify @charset/@character user variables by their names.
func validateCharsetAssignment(assign *tree.VarAssignmentExpr, value interface{}) error {
	if !assign.SetNames && !assign.CharsetRequest {
		return nil
	}
	if _, isDefault := assign.Value.(*tree.DefaultVal); isDefault {
		return nil
	}
	if err := validateCharsetSystemVariable("character_set_connection", value); err != nil {
		return err
	}
	if assign.Reserved != nil {
		literal, ok := assign.Reserved.(*tree.NumVal)
		if !ok {
			return moerr.NewInvalidInputNoCtx("SET NAMES requires a constant collation")
		}
		return validateCharsetCollationPair(value, literal.String())
	}
	return nil
}

// validateCharsetCollationPair checks the effective charset/collation pair
// before SET NAMES publishes any session state.  This is also needed for
// SET NAMES DEFAULT: the two server defaults are independently configurable,
// so an old or manually changed catalog can contain an incompatible pair.
func validateCharsetCollationPair(charsetValue interface{}, collationName string) error {
	charsetName, ok := charsetValue.(string)
	if !ok {
		return moerr.NewInvalidInputNoCtx("character set must be a string")
	}
	charsetID, ok := collation.ResolveCharset(charsetName)
	if !ok {
		return moerr.NewInvalidInputNoCtxf("unsupported character set '%s'", charsetValue)
	}
	collationID, collationRevision, ok := collation.ResolveSession(collationName)
	if !ok {
		return moerr.NewInvalidInputNoCtxf("unsupported collation '%s'", collationName)
	}
	charset, _ := collation.EffectiveDefinition(uint32(charsetID), 0)
	coll, _ := collation.EffectiveDefinition(uint32(collationID), uint32(collationRevision))
	// ResolveCharset intentionally preserves MatrixOne's historical utf8
	// compatibility alias as utf8mb4. Native UCA400 collations carry their
	// actual charset, so compare the requested spelling directly here rather
	// than accidentally rejecting utf8_unicode_ci or accepting a mismatch.
	if coll.Semantics == collation.UCA400 {
		requestedCharset := collation.CanonicalCharsetName(charsetName)
		if requestedCharset != coll.Charset.Name() {
			return moerr.NewInvalidInputNoCtxf("COLLATION '%s' is not valid for CHARACTER SET '%s'", collationName, charsetValue)
		}
		return nil
	}
	if charset.Charset != coll.Charset {
		return moerr.NewInvalidInputNoCtxf("COLLATION '%s' is not valid for CHARACTER SET '%s'", collationName, charsetValue)
	}
	return nil
}

// resolveImplicitSetNamesCollation returns the collation that MySQL applies
// when SET NAMES changes a charset without an explicit COLLATE clause. The
// utf8mb4 default is configurable; the other admitted charset defaults are
// owned by the collation registry.
func resolveImplicitSetNamesCollation(ses *Session, charsetValue interface{}) (string, error) {
	charsetName, ok := charsetValue.(string)
	if !ok {
		return "", moerr.NewInvalidInputNoCtx("character set must be a string")
	}
	charsetName = collation.CanonicalCharsetName(strings.TrimSpace(charsetName))
	if charsetName == "utf8mb4" {
		value, err := ses.GetSessionSysVar("default_collation_for_utf8mb4")
		if err != nil {
			return "", err
		}
		if value == nil {
			value = gSysVarsDefs["default_collation_for_utf8mb4"].Default
		}
		collationName, ok := value.(string)
		if !ok {
			return "", moerr.NewInvalidInputNoCtx("default collation for utf8mb4 must be a string")
		}
		return collationName, nil
	}
	for _, definition := range collation.Definitions() {
		if definition.Default && definition.Charset.Name() == charsetName {
			return definition.Name, nil
		}
	}
	return "", moerr.NewInvalidInputNoCtxf("unsupported character set '%s'", charsetValue)
}

func resolveSetNamesDefaultCollation(ses *Session) (string, error) {
	value, err := ses.GetGlobalSysVar("collation_server")
	if err != nil {
		return "", err
	}
	if value == nil {
		value = gSysVarsDefs["collation_server"].Default
	}
	collationName, ok := value.(string)
	if !ok {
		return "", moerr.NewInvalidInputNoCtx("server collation must be a string")
	}
	return collationName, nil
}

func lookupSupportedProtocolCollation(id int) (charsetCollationName, bool) {
	if id < 0 || id > 65535 {
		return charsetCollationName{}, false
	}
	d, ok := collation.LookupProtocol(uint16(id))
	if !ok || !collation.IsProtocolAdmitted(d) {
		return charsetCollationName{}, false
	}
	return charsetCollationName{charset: d.Charset.Name(), collationName: d.Name}, true
}
