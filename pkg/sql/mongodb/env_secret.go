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

package mongodb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// EnvSecretResolver is a minimal resolver for local deployments and CI. The
// credential environment value is JSON with username/password fields; the TLS
// environment value is PEM. Production deployments can replace this runtime
// dependency with their KMS/secret-manager resolver.
type EnvSecretResolver struct{}

func (EnvSecretResolver) ResolveMongoDBCredentials(
	ctx context.Context,
	accountID uint32,
	credentialRef string,
	tlsCARef string,
) (Credentials, error) {
	credentialKey, err := envSecretKey(ctx, accountID, credentialRef)
	if err != nil {
		return Credentials{}, err
	}
	raw, ok := os.LookupEnv(credentialKey)
	if !ok {
		return Credentials{}, moerr.NewInternalError(ctx, "MongoDB credential secret is unavailable")
	}
	var payload struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil || payload.Username == "" || payload.Password == "" {
		return Credentials{}, moerr.NewInternalError(ctx, "MongoDB credential secret has an invalid format")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Credentials{}, moerr.NewInternalError(ctx, "MongoDB credential secret has an invalid format")
	}
	credentials := Credentials{Username: payload.Username, Password: payload.Password}
	if strings.TrimSpace(tlsCARef) != "" {
		caKey, err := envSecretKey(ctx, accountID, tlsCARef)
		if err != nil {
			return Credentials{}, err
		}
		ca, ok := os.LookupEnv(caKey)
		if !ok {
			return Credentials{}, moerr.NewInternalError(ctx, "MongoDB TLS CA secret is unavailable")
		}
		credentials.TLSCA = []byte(ca)
	}
	return credentials, nil
}

func envSecretKey(ctx context.Context, accountID uint32, ref string) (string, error) {
	const prefix = "secret://env/"
	if !strings.HasPrefix(ref, prefix) || len(ref) == len(prefix) {
		return "", moerr.NewNotSupported(ctx, "default MongoDB resolver only supports secret://env references")
	}
	key := strings.TrimPrefix(ref, prefix)
	if strings.ContainsAny(key, "/\\\x00=") {
		return "", moerr.NewInvalidInput(ctx, "MongoDB environment secret reference is invalid")
	}
	allowedPrefix := "MO_MONGODB_"
	if accountID != 0 {
		allowedPrefix = fmt.Sprintf("MO_MONGODB_ACCOUNT_%d_", accountID)
	}
	if !strings.HasPrefix(key, allowedPrefix) {
		return "", moerr.NewInvalidInput(ctx, "MongoDB environment secret reference is outside the account-scoped namespace")
	}
	return key, nil
}
