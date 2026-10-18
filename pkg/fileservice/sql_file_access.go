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

package fileservice

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type sqlFileAccessKey struct{}
type sqlFileAccessPolicy struct{ serverData bool }

// WithSQLFileAccess binds a statement identity, never an account-number grant.
// The snapshot must be rebound after the caller installs its current identity.
func WithSQLFileAccess(ctx context.Context, account, role string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	policy := sqlFileAccessPolicy{serverData: strings.ToLower(account) == "sys" && strings.ToLower(role) == "moadmin"}
	if old, ok := ctx.Value(sqlFileAccessKey{}).(sqlFileAccessPolicy); ok && old == policy {
		return ctx
	}
	return context.WithValue(ctx, sqlFileAccessKey{}, policy)
}

// SQLFilePathRequiresAdmin classifies every dependency without acquiring it.
// Dynamic SQL providers may use caller credentials, never server auxiliaries.
func SQLFilePathRequiresAdmin(path Path) (bool, error) {
	_, server, err := sqlFilePathResources(path, true)
	return server, err
}

func sqlFilePathResources(path Path, forETL bool) (args ObjectStorageArguments, server bool, err error) {
	if path.Service == "" || len(path.ServiceArguments) == 0 {
		return args, true, nil
	}
	args, err = parseFileServiceArguments(path, forETL)
	if err != nil {
		return args, false, err
	}
	if err = args.validateSQLResources(); err != nil {
		return args, false, err
	}
	args.callerCredentialsOnly = true
	return args, selectObjectStorageBackend(args) == objectStorageDisk, nil
}

func (o ObjectStorageArguments) validateSQLResources() error {
	if len(o.CertFiles) > 0 || o.KerberosKeytabPath != "" {
		return moerr.NewNotSupportedNoCtx("SQL dynamic providers cannot use server-local auxiliary files")
	}
	if o.SharedConfigProfile != "" || o.RAMRole != "" {
		return moerr.NewNotSupportedNoCtx("SQL dynamic providers cannot use server credential discovery")
	}
	backend := selectObjectStorageBackend(o)
	if backend == objectStorageDisk {
		return nil
	}
	if o.RoleARN != "" && backend != objectStorageAWS && backend != objectStorageAliyun && backend != objectStorageMinio {
		return moerr.NewNotSupportedNoCtx("AssumeRole is not supported by this SQL provider")
	}
	if backend == objectStorageHDFS {
		// HDFS simple authentication uses a supplied user or the literal
		// "hadoop" default, not a server credential/configuration source.
		// Keep endpoint-only HDFS URLs and stages working. Password options
		// are interpreted by the existing HDFS constructor; keytabs are rejected above.
		return nil
	}
	if o.KeyID != "" && o.KeySecret != "" {
		return nil
	}
	if backend == objectStorageAliyun && o.BearerToken != "" && o.KeyID == "" && o.KeySecret == "" && o.RoleARN == "" {
		return nil
	}
	if backend == objectStorageMinio && o.NoDefaultCredentials &&
		o.KeyID == "" && o.KeySecret == "" && o.SessionToken == "" &&
		o.SecurityToken == "" && o.BearerToken == "" && o.RoleARN == "" {
		// The existing explicit no-default-credentials mode with an empty
		// MinIO credential chain means anonymous access. Partial credentials
		// and absent credentials without explicit intent must still fail.
		return nil
	}
	return moerr.NewInvalidInputNoCtx("SQL dynamic object storage requires explicit credentials")
}

func admitSQLFilePath(ctx context.Context, path Path, forETL bool) (ObjectStorageArguments, error) {
	if ctx == nil {
		return ObjectStorageArguments{}, moerr.NewInternalErrorNoCtx("SQL file access policy is required")
	}
	policy, ok := ctx.Value(sqlFileAccessKey{}).(sqlFileAccessPolicy)
	if !ok {
		return ObjectStorageArguments{}, moerr.NewInternalErrorNoCtx("SQL file access policy is required")
	}
	if err := ctx.Err(); err != nil {
		return ObjectStorageArguments{}, moerr.ConvertGoError(ctx, err)
	}
	return policy.admit(path, forETL)
}

func (p sqlFileAccessPolicy) admit(path Path, forETL bool) (ObjectStorageArguments, error) {
	args, server, err := sqlFilePathResources(path, forETL)
	if err != nil {
		return args, err
	}
	if server && !p.serverData {
		return args, moerr.NewInternalErrorNoCtx("only the sys account moadmin role can access SQL file paths")
	}
	return args, nil
}

// AuthorizeSQLFileAccessPath is the pure preflight form of factory admission.
func AuthorizeSQLFileAccessPath(account, role string, path Path) error {
	_, err := (sqlFileAccessPolicy{serverData: strings.ToLower(account) == "sys" && strings.ToLower(role) == "moadmin"}).admit(path, true)
	return err
}
