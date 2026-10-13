// Copyright 2022 Matrix Origin
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
	"net/url"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type exclusiveWriteKey struct{}

func exclusiveWrite(ctx context.Context) bool { return ctx.Value(exclusiveWriteKey{}) == true }

// CheckExclusiveWrite rejects destinations whose writer cannot create without
// replacing an existing file. Call before preparing irreversible effects.
func CheckExclusiveWrite(fs FileService) error {
	switch f := fs.(type) {
	case *subPathFS:
		return CheckExclusiveWrite(f.upstream)
	case *LocalETLFS:
		return nil
	case *S3FS:
		switch storage := f.rawStorage.(type) {
		case *AwsSDKv2:
			if awsS3ConditionalPutEndpoint(storage.endpoint) {
				return nil
			}
		case *diskObjectStorage:
			return nil
		case *MinioSDK:
			endpoint := strings.ToLower(storage.endpoint)
			// These cloud endpoints are routed through the MinIO SDK for
			// compatibility, but their conditional-PUT behavior has not been
			// verified. Do not treat SDK selection as a capability guarantee.
			if !strings.Contains(endpoint, "ctyunapi.cn") &&
				!strings.Contains(endpoint, "myqcloud.com") &&
				!strings.Contains(endpoint, "aliyuncs.com") {
				return nil
			}
		}
	}
	return moerr.NewNotSupportedNoCtxf("exclusive publication for %T", fs)
}

// awsS3ConditionalPutEndpoint limits the AWS SDK path to Amazon-owned
// endpoints. The SDK also accepts arbitrary S3-compatible endpoints, whose
// handling of If-None-Match is not guaranteed by the S3 API contract.
func awsS3ConditionalPutEndpoint(endpoint string) bool {
	if endpoint == "" {
		return true // AWS SDK's default S3 endpoint.
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	return strings.HasSuffix(host, ".amazonaws.com") ||
		strings.HasSuffix(host, ".amazonaws.com.cn")
}

// WriteExclusive creates a bounded, known-size file without replacing a prior
// publisher, even if a SQL lock binding is lost. It uses the existing writer,
// including its storage mappings, metrics and cleanup. There is no unsafe
// check-then-write fallback. Ambiguous errors retain the file and fail closed.
func WriteExclusive(ctx context.Context, fs FileService, vector IOVector) error {
	if err := CheckExclusiveWrite(fs); err != nil {
		return err
	}
	size := vector.size()
	if size == nil || *size > smallObjectThreshold {
		return moerr.NewNotSupportedNoCtx("exclusive publication requires a known size at most 64 MiB")
	}
	return fs.Write(context.WithValue(ctx, exclusiveWriteKey{}, true), vector)
}
