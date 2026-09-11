// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package udf

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/udf/protocol"
)

const (
	pythonInlineArtifactDigestDomain = "matrixone-python-inline-artifact"
	pythonEnvironmentDigestDomain    = "matrixone-python-environment"
)

// PythonInlineArtifactDigest identifies the exact source payload sent to the
// worker. Length-prefixing each UTF-8 string prevents delimiter ambiguity and
// gives the Python adapter an unambiguous, language-independent algorithm.
func PythonInlineArtifactDigest(handler, source string) string {
	return pythonContractDigest(pythonInlineArtifactDigestDomain, handler, source)
}

// PythonEnvironmentDigest identifies the worker-side contract that interprets
// an inline source definition. The current environment plane is deliberately
// small: the executable artifact is inline, while the ABI, plan, type and
// timezone contracts are fixed by the negotiated worker capability. A future
// artifact store may add its immutable environment digest without changing
// this current digest algorithm.
func PythonEnvironmentDigest() (string, error) {
	tzVersion, err := TimezoneDatabaseVersion()
	if err != nil {
		return "", err
	}
	return pythonContractDigest(
		pythonEnvironmentDigestDomain,
		fmt.Sprintf("%d", protocol.Version),
		PythonABIContract,
		PythonAdapterVersion,
		PythonSDKVersion,
		fmt.Sprintf("%d", PythonDefinitionSchemaVersion),
		fmt.Sprintf("%d", PythonPlanContractVersion),
		PythonTypeDescriptorContract,
		tzVersion,
	), nil
}

// IsSHA256Digest is used at execution boundaries for persisted digests and
// fingerprints. It accepts the canonical lower-case hexadecimal spelling
// only, so malformed or alternate encodings cannot create ambiguous identity.
func IsSHA256Digest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return false
	}
	for _, ch := range value {
		if ch >= 'A' && ch <= 'F' {
			return false
		}
	}
	return true
}

func pythonContractDigest(domain string, parts ...string) string {
	hash := sha256.New()
	writePythonDigestPart(hash, domain)
	for _, part := range parts {
		writePythonDigestPart(hash, part)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func writePythonDigestPart(hash interface{ Write([]byte) (int, error) }, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write([]byte(value))
}
