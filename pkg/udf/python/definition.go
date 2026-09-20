// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package python

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// definitionFingerprintBody is the canonical executable contract.  Source is
// deliberately absent: original source is a catalog/audit payload, while the
// immutable artifact digest identifies the exact bytes that may be executed.
// Changing a type or execution policy must invalidate the definition even
// when the artifact bytes stay unchanged.
type definitionFingerprintBody struct {
	DefinitionSchemaVersion int              `json:"definition_schema_version"`
	Handler                 string           `json:"handler"`
	Mode                    string           `json:"mode"`
	NullPolicy              string           `json:"null_policy"`
	ABIContract             string           `json:"abi_contract"`
	AdapterVersion          string           `json:"adapter_version"`
	ArtifactDigest          string           `json:"artifact_digest"`
	EnvironmentDigest       string           `json:"environment_digest"`
	SDKVersion              string           `json:"sdk_version"`
	ArgTypes                []TypeDescriptor `json:"arg_types,omitempty"`
	ReturnType              *TypeDescriptor  `json:"return_type,omitempty"`
}

// DefinitionFingerprint computes the canonical current Python definition
// fingerprint.  Callers must pass descriptors produced by NewTypeDescriptor;
// validating here makes the worker and Gateway reject an identity whose type
// metadata is not part of the supported Arrow contract before user code runs.
func DefinitionFingerprint(
	definitionSchemaVersion int,
	handler, mode, nullPolicy, abiContract, adapterVersion string,
	artifactDigest, environmentDigest, sdkVersion string,
	args []TypeDescriptor,
	returnType TypeDescriptor,
) (string, error) {
	if definitionSchemaVersion <= 0 || handler == "" ||
		mode == "" || nullPolicy == "" || abiContract == "" ||
		adapterVersion == "" || sdkVersion == "" || artifactDigest == "" ||
		environmentDigest == "" {
		return "", fmt.Errorf("python definition fingerprint has incomplete metadata")
	}
	for index, descriptor := range args {
		if err := descriptor.Validate(); err != nil {
			return "", fmt.Errorf("argument descriptor %d: %w", index, err)
		}
	}
	if err := returnType.Validate(); err != nil {
		return "", fmt.Errorf("return descriptor: %w", err)
	}
	body := definitionFingerprintBody{
		DefinitionSchemaVersion: definitionSchemaVersion,
		Handler:                 handler,
		Mode:                    mode,
		NullPolicy:              nullPolicy,
		ABIContract:             abiContract,
		AdapterVersion:          adapterVersion,
		ArtifactDigest:          artifactDigest,
		EnvironmentDigest:       environmentDigest,
		SDKVersion:              sdkVersion,
		ArgTypes:                args,
		ReturnType:              &returnType,
	}
	canonical, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("marshal definition fingerprint: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}
