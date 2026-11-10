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
	"regexp"
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
			// The MinIO SDK also targets compatibility endpoints. Only the
			// explicit MinIO configuration opts into the conditional-PUT
			// guarantee; SDK selection alone is not a capability guarantee.
			if storage.supportsConditionalPut {
				return nil
			}
		}
	}
	return moerr.NewNotSupportedNoCtxf("exclusive publication for %T", fs)
}

// Match S3 API endpoints, optionally prefixed by a virtual-hosted bucket.
// Other AWS services (including EC2 and API Gateway) do not inherit S3's
// conditional-PUT guarantee merely by sharing the amazonaws.com suffix.
// Unknown endpoint forms fail closed.
var awsS3APIHost = regexp.MustCompile(`(^|\.)s3(?:\.amazonaws\.com|(?:[.-]|-fips\.)(?:dualstack\.)?[a-z]{2}(?:-gov)?-[a-z]+-[0-9]+\.amazonaws\.com(?:\.cn)?)$`)

// awsS3ConditionalPutEndpoint limits the AWS SDK path to S3 service endpoints.
// See https://docs.aws.amazon.com/general/latest/gr/s3.html.
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
	return awsS3APIHost.MatchString(host)
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
