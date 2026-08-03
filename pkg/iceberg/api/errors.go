// Copyright 2026 Matrix Origin
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

package api

import (
	"context"
	stderrors "errors"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type ErrorCode string

const (
	ErrFeatureDisabled        ErrorCode = "ICEBERG_FEATURE_DISABLED"
	ErrConfigInvalid          ErrorCode = "ICEBERG_CONFIG_INVALID"
	ErrAuthUnauthorized       ErrorCode = "ICEBERG_AUTH_UNAUTHORIZED"
	ErrAuthForbidden          ErrorCode = "ICEBERG_AUTH_FORBIDDEN"
	ErrAuthTimeout            ErrorCode = "ICEBERG_AUTH_TIMEOUT"
	ErrPrincipalNotMapped     ErrorCode = "ICEBERG_PRINCIPAL_NOT_MAPPED"
	ErrResidencyDenied        ErrorCode = "ICEBERG_RESIDENCY_DENIED"
	ErrTableNotFound          ErrorCode = "ICEBERG_TABLE_NOT_FOUND"
	ErrCatalogUnavailable     ErrorCode = "ICEBERG_CATALOG_UNAVAILABLE"
	ErrMetadataIOTimeout      ErrorCode = "ICEBERG_METADATA_IO_TIMEOUT"
	ErrMetadataInvalid        ErrorCode = "ICEBERG_METADATA_INVALID"
	ErrUnsupportedFeature     ErrorCode = "ICEBERG_UNSUPPORTED_FEATURE"
	ErrPlanningTimeout        ErrorCode = "ICEBERG_PLANNING_TIMEOUT"
	ErrPlanningLimitExceeded  ErrorCode = "ICEBERG_PLANNING_LIMIT_EXCEEDED"
	ErrServerPlanningRequired ErrorCode = "ICEBERG_SERVER_PLANNING_REQUIRED"
	ErrCredentialExpired      ErrorCode = "ICEBERG_CREDENTIAL_EXPIRED"
	ErrRemoteSigningDenied    ErrorCode = "ICEBERG_REMOTE_SIGNING_DENIED"
	ErrRemoteSigningExpired   ErrorCode = "ICEBERG_REMOTE_SIGNING_EXPIRED"
	ErrCommitConflict         ErrorCode = "ICEBERG_COMMIT_CONFLICT"
	ErrCommitUnknown          ErrorCode = "ICEBERG_COMMIT_UNKNOWN"
	ErrOrphanCleanupFailed    ErrorCode = "ICEBERG_ORPHAN_CLEANUP_FAILED"
	ErrObjectIO               ErrorCode = "ICEBERG_OBJECT_IO"
	ErrInternal               ErrorCode = "ICEBERG_INTERNAL"
)

type IcebergError struct {
	Code    ErrorCode
	Message string
	Fields  map[string]string
	Cause   error
}

func NewError(code ErrorCode, message string, fields map[string]string) *IcebergError {
	return &IcebergError{
		Code:    code,
		Message: message,
		Fields:  RedactFields(fields),
	}
}

func WrapError(code ErrorCode, message string, fields map[string]string, cause error) *IcebergError {
	err := NewError(code, message, fields)
	err.Cause = cause
	return err
}

func (e *IcebergError) Error() string {
	if e == nil {
		return ""
	}
	var b strings.Builder
	b.WriteString(string(e.Code))
	if e.Message != "" {
		b.WriteString(": ")
		b.WriteString(e.Message)
	}
	if len(e.Fields) > 0 {
		keys := make([]string, 0, len(e.Fields))
		for k := range e.Fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteString(" [")
		for i, k := range keys {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(k)
			b.WriteString("=")
			b.WriteString(e.Fields[k])
		}
		b.WriteString("]")
	}
	return b.String()
}

func (e *IcebergError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func ToMOErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	var mo *moerr.Error
	if stderrors.As(err, &mo) {
		return err
	}
	var icebergErr *IcebergError
	if !stderrors.As(err, &icebergErr) {
		return moerr.NewInternalError(ctx, err.Error())
	}
	msg := icebergErr.Error()
	switch icebergErr.Code {
	case ErrConfigInvalid, ErrCredentialExpired:
		return moerr.NewBadConfig(ctx, msg)
	case ErrFeatureDisabled, ErrUnsupportedFeature, ErrServerPlanningRequired:
		return moerr.NewNotSupported(ctx, msg)
	case ErrAuthUnauthorized, ErrAuthForbidden, ErrAuthTimeout, ErrPrincipalNotMapped, ErrResidencyDenied:
		return moerr.NewInvalidInput(ctx, msg)
	case ErrCatalogUnavailable, ErrMetadataIOTimeout, ErrTableNotFound, ErrMetadataInvalid, ErrPlanningTimeout, ErrPlanningLimitExceeded, ErrObjectIO:
		return moerr.NewInvalidState(ctx, msg)
	case ErrRemoteSigningDenied, ErrRemoteSigningExpired:
		return moerr.NewInvalidState(ctx, msg)
	case ErrCommitConflict, ErrCommitUnknown, ErrOrphanCleanupFailed:
		return moerr.NewInvalidState(ctx, msg)
	default:
		return moerr.NewInternalError(ctx, msg)
	}
}

func CauseForCode(code ErrorCode) error {
	switch code {
	case ErrConfigInvalid, ErrFeatureDisabled:
		return moerr.CauseIcebergConfig
	case ErrCatalogUnavailable, ErrAuthUnauthorized, ErrAuthForbidden, ErrAuthTimeout:
		return moerr.CauseIcebergCatalog
	case ErrPrincipalNotMapped:
		return moerr.CauseIcebergCredential
	case ErrResidencyDenied:
		return moerr.CauseIcebergResidency
	case ErrCredentialExpired, ErrRemoteSigningDenied, ErrRemoteSigningExpired:
		return moerr.CauseIcebergCredential
	case ErrMetadataInvalid, ErrTableNotFound, ErrMetadataIOTimeout:
		return moerr.CauseIcebergMetadata
	case ErrPlanningTimeout, ErrPlanningLimitExceeded, ErrServerPlanningRequired:
		return moerr.CauseIcebergPlanning
	case ErrUnsupportedFeature:
		return moerr.CauseIcebergMetadata
	default:
		return moerr.CauseIcebergInternal
	}
}
