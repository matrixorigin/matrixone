// Copyright 2023 Matrix Origin
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
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	gotrace "runtime/trace"
	"strconv"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/aliyun/credentials-go/credentials"
	awscredentials "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

type AliyunSDK struct {
	name            string
	bucket          *oss.Bucket
	client          *oss.Client
	perfCounterSets []*perfcounter.CounterSet
	listMaxKeys     int
}

func NewAliyunSDK(
	ctx context.Context,
	args ObjectStorageArguments,
	perfCounterSets []*perfcounter.CounterSet,
) (*AliyunSDK, error) {

	if err := args.validate(); err != nil {
		return nil, err
	}

	// env vars
	if args.OIDCRoleARN == "" {
		if v := os.Getenv("ALIBABA_CLOUD_ROLE_ARN"); v != "" {
			args.OIDCRoleARN = v
		}
	}
	if args.OIDCProviderARN == "" {
		if v := os.Getenv("ALIBABA_CLOUD_OIDC_PROVIDER_ARN"); v != "" {
			args.OIDCProviderARN = v
		}
	}
	if args.OIDCTokenFilePath == "" {
		if v := os.Getenv("ALIBABA_CLOUD_OIDC_TOKEN_FILE"); v != "" {
			args.OIDCTokenFilePath = v
		}
	}

	opts := []oss.ClientOption{}
	if args.Region != "" {
		opts = append(opts, oss.Region(args.Region))
	}
	if args.SecurityToken != "" {
		opts = append(opts, oss.SecurityToken(args.SecurityToken))
	}
	credentialsProvider, err := args.credentialProviderForAliyunSDK(ctx)
	if err != nil {
		return nil, err
	}
	if credentialsProvider != nil {
		opts = append(opts, oss.SetCredentialsProvider(credentialsProvider))
	}

	client, err := oss.New(
		args.Endpoint,
		"", "",
		opts...,
	)
	if err != nil {
		return nil, err
	}

	bucket, err := client.Bucket(args.Bucket)
	if err != nil {
		return nil, err
	}

	logutil.Info("new object storage",
		zap.Any("sdk", "aliyun"),
		zap.Any("endpoint", args.Endpoint),
		zap.Any("bucket", args.Bucket),
	)

	return &AliyunSDK{
		name:            args.Name,
		client:          client,
		bucket:          bucket,
		perfCounterSets: perfCounterSets,
	}, nil
}

var _ ObjectStorage = new(AliyunSDK)

func (a *AliyunSDK) List(
	ctx context.Context,
	prefix string,
	fn func(bool, string, int64) (bool, error),
) error {

	if err := ctx.Err(); err != nil {
		return err
	}

	var cont string

loop1:
	for {
		result, err := a.listObjects(ctx, prefix, cont)
		if err != nil {
			return err
		}

		for _, obj := range result.Objects {
			more, err := fn(false, obj.Key, obj.Size)
			if err != nil {
				return err
			}
			if !more {
				break loop1
			}
		}

		for _, prefix := range result.CommonPrefixes {
			more, err := fn(true, prefix, 0)
			if err != nil {
				return err
			}
			if !more {
				break loop1
			}
		}

		if !result.IsTruncated {
			break
		}
		cont = result.NextContinuationToken
	}

	return nil
}

func (a *AliyunSDK) Stat(
	ctx context.Context,
	key string,
) (
	size int64,
	err error,
) {

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	info, err := a.statObject(
		ctx,
		key,
	)
	if err != nil {
		return
	}

	if str := info.Get(oss.HTTPHeaderContentLength); str != "" {
		size, err = strconv.ParseInt(str, 10, 64)
		if err != nil {
			return
		}
	}

	return
}

func (a *AliyunSDK) Exists(
	ctx context.Context,
	key string,
) (
	bool,
	error,
) {

	if err := ctx.Err(); err != nil {
		return false, err
	}

	_, err := a.statObject(
		ctx,
		key,
	)
	if err != nil {
		if a.is404(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (a *AliyunSDK) Write(
	ctx context.Context,
	key string,
	r io.Reader,
	size int64,
	expire *time.Time,
) (
	err error,
) {

	_, err = a.putObject(
		ctx,
		key,
		r,
		size,
		expire,
	)
	if err != nil {
		return err
	}

	return
}

func (a *AliyunSDK) Read(
	ctx context.Context,
	key string,
	min *int64,
	max *int64,
) (
	r io.ReadCloser,
	err error,
) {

	defer func() {
		if a.is404(err) {
			err = moerr.NewFileNotFoundNoCtx(key)
		}
	}()

	if max == nil {
		// read to end
		r, err := a.getObject(
			ctx,
			key,
			min,
			nil,
		)
		if err != nil {
			return nil, err
		}
		return r, nil
	}

	r, err = a.getObject(
		ctx,
		key,
		min,
		max,
	)
	if err != nil {
		return nil, err
	}
	return &readCloser{
		r:         io.LimitReader(r, int64(*max-*min)),
		closeFunc: r.Close,
	}, nil
}

func (a *AliyunSDK) Delete(
	ctx context.Context,
	keys ...string,
) (
	err error,
) {

	if err := ctx.Err(); err != nil {
		return err
	}

	if len(keys) == 0 {
		return nil
	}
	if len(keys) == 1 {
		return a.deleteSingle(ctx, keys[0])
	}

	for i := 0; i < len(keys); i += 1000 {
		end := i + 1000
		if end > len(keys) {
			end = len(keys)
		}
		if _, err := a.deleteObjects(ctx, keys[i:end]...); err != nil {
			return err
		}
	}

	return nil
}

func (a *AliyunSDK) deleteSingle(ctx context.Context, key string) error {
	ctx, span := trace.Start(ctx, "AliyunSDK.deleteSingle")
	defer span.End()

	_, err := a.deleteObject(
		ctx,
		key,
	)
	if err != nil {
		return err
	}

	return nil
}

func (a *AliyunSDK) listObjects(ctx context.Context, prefix string, cont string) (oss.ListObjectsResultV2, error) {
	ctx, task := gotrace.NewTask(ctx, "AliyunSDK.listObjects")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.List.Add(1)
	}, a.perfCounterSets...)
	opts := []oss.Option{
		oss.WithContext(ctx),
		oss.Delimiter("/"),
	}
	if prefix != "" {
		opts = append(opts, oss.Prefix(prefix))
	}
	if cont != "" {
		opts = append(opts, oss.ContinuationToken(cont))
	}
	if a.listMaxKeys > 0 {
		opts = append(opts, oss.MaxKeys(a.listMaxKeys))
	}
	return doWithRetry(
		"s3 list objects",
		func() (oss.ListObjectsResultV2, error) {
			return a.bucket.ListObjectsV2(opts...)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *AliyunSDK) statObject(ctx context.Context, key string) (http.Header, error) {
	ctx, task := gotrace.NewTask(ctx, "AliyunSDK.statObject")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Head.Add(1)
	}, a.perfCounterSets...)
	return doWithRetry(
		"s3 head object",
		func() (http.Header, error) {
			return a.bucket.GetObjectMeta(
				key,
				oss.WithContext(ctx),
			)
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *AliyunSDK) putObject(
	ctx context.Context,
	key string,
	r io.Reader,
	size int64,
	expire *time.Time,
) (any, error) {
	ctx, task := gotrace.NewTask(ctx, "AliyunSDK.putObject")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Put.Add(1)
	}, a.perfCounterSets...)
	// not retryable because Reader may be half consumed
	opts := []oss.Option{
		oss.WithContext(ctx),
	}
	if expire != nil {
		opts = append(opts, oss.Expires(*expire))
	}
	return a.bucket.PutObject(
		key,
		r,
		opts...,
	), nil
}

func (a *AliyunSDK) getObject(ctx context.Context, key string, min *int64, max *int64) (io.ReadCloser, error) {
	ctx, task := gotrace.NewTask(ctx, "AliyunSDK.getObject")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Get.Add(1)
	}, a.perfCounterSets...)
	r, err := newRetryableReader(
		func(offset int64) (io.ReadCloser, error) {
			opts := []oss.Option{
				oss.WithContext(ctx),
			}
			var rang string
			if max != nil {
				rang = fmt.Sprintf("%d-%d", offset, *max)
			} else {
				rang = fmt.Sprintf("%d-", offset)
			}
			opts = append(opts, oss.NormalizedRange(rang))
			opts = append(opts, oss.RangeBehavior("standard"))
			r, err := doWithRetry(
				"s3 get object",
				func() (io.ReadCloser, error) {
					return a.bucket.GetObject(
						key,
						opts...,
					)
				},
				maxRetryAttemps,
				isRetryableError,
			)
			if err != nil {
				return nil, err
			}
			return r, nil
		},
		*min,
		isRetryableError,
	)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (a *AliyunSDK) deleteObject(ctx context.Context, key string) (any, error) {
	ctx, task := gotrace.NewTask(ctx, "AliyunSDK.deleteObject")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Delete.Add(1)
	}, a.perfCounterSets...)
	return doWithRetry(
		"s3 delete object",
		func() (any, error) {
			if err := a.bucket.DeleteObject(
				key,
				oss.WithContext(ctx),
			); err != nil {
				return nil, err
			}
			return nil, nil
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *AliyunSDK) deleteObjects(ctx context.Context, keys ...string) (any, error) {
	ctx, task := gotrace.NewTask(ctx, "AliyunSDK.deleteObjects")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.DeleteMulti.Add(1)
	}, a.perfCounterSets...)
	return doWithRetry(
		"s3 delete objects",
		func() (any, error) {
			_, err := a.bucket.DeleteObjects(
				keys,
				oss.WithContext(ctx),
			)
			if err != nil {
				return nil, err
			}
			return nil, nil
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *AliyunSDK) is404(err error) bool {
	if err == nil {
		return false
	}
	var ossErr oss.ServiceError
	if errors.As(err, &ossErr) {
		if ossErr.Code == "NoSuchKey" {
			return true
		}
	}
	return false
}

func (o ObjectStorageArguments) credentialProviderForAliyunSDK(
	ctx context.Context,
) (ret oss.CredentialsProvider, err error) {

	defer func() {
		if err != nil {
			return
		}
		if o.AssumeRoleARN != "" {
			// assume ram role
			logutil.Info("aliyun sdk credential", zap.Any("using", "assume ram role"))
			if ret == nil {
				err = moerr.NewBadConfig(ctx, "ram role arn without access key")
				return
			}
			creds := ret.GetCredentials()
			conf := &credentials.Config{
				Type:            ptrTo("ram_role_arn"),
				AccessKeyId:     ptrTo(creds.GetAccessKeyID()),
				AccessKeySecret: ptrTo(creds.GetAccessKeySecret()),
				RoleArn:         ptrTo(o.AssumeRoleARN),
			}

			if o.RoleSessionName != "" {
				conf.RoleSessionName = &o.RoleSessionName
			}
			if o.ExternalID != "" {
				conf.ExternalId = &o.ExternalID
			}

			var provider credentials.Credential
			provider, err = credentials.NewCredential(conf)
			if err != nil {
				logutil.Error("aliyun credential error", zap.Error(err))
				return
			}
			ret = aliyunCredentialsProviderFunc(func() (string, string, string) {
				v, err := provider.GetCredential()
				if err != nil {
					logutil.Error("aliyun credential error", zap.Error(err))
					return "", "", ""
				}
				return *v.AccessKeyId, *v.AccessKeySecret, *v.SecurityToken
			})
		}
	}()

	// static
	if o.KeyID != "" && o.KeySecret != "" {
		logutil.Info("aliyun sdk credential", zap.Any("using", "static"))
		return aliyunCredentialsProviderFunc(func() (string, string, string) {
			return o.KeyID, o.KeySecret, o.SecurityToken
		}), nil
	}

	// bearer token
	if o.BearerToken != "" {
		logutil.Info("aliyun sdk credential", zap.Any("using", "bearer token"))
		provider, err := credentials.NewCredential(&credentials.Config{
			Type:        ptrTo("bearer"),
			BearerToken: &o.BearerToken,
		})
		if err != nil {
			return nil, err
		}
		return aliyunCredentialsProviderFunc(func() (string, string, string) {
			v, err := provider.GetCredential()
			if err != nil {
				logutil.Error("aliyun credential error", zap.Error(err))
				return "", "", ""
			}
			return *v.AccessKeyId, *v.AccessKeySecret, *v.SecurityToken
		}), nil
	}

	// ram role
	if o.RAMRole != "" {
		provider, err := credentials.NewCredential(&credentials.Config{
			Type:     ptrTo("ecs_ram_role"),
			RoleName: ptrTo(o.RAMRole),
		})
		if err != nil {
			return nil, err
		}
		logutil.Info("aliyun sdk credential", zap.Any("using", "ecs ram role"))
		return aliyunCredentialsProviderFunc(func() (string, string, string) {
			v, err := provider.GetCredential()
			if err != nil {
				logutil.Error("aliyun credential error", zap.Error(err))
				return "", "", ""
			}
			return *v.AccessKeyId, *v.AccessKeySecret, *v.SecurityToken
		}), nil
	}

	// from env
	provider, err := oss.NewEnvironmentVariableCredentialsProvider()
	if err == nil {
		logutil.Info("aliyun sdk credential", zap.Any("using", "env"))
		return &provider, nil
	}

	// from aws env
	awsCredentials := awscredentials.NewEnvCredentials()
	v, err := awsCredentials.Get()
	if err == nil {
		logutil.Info("aliyun sdk credential", zap.Any("using", "aws env"))
		return aliyunCredentialsProviderFunc(func() (string, string, string) {
			return v.AccessKeyID, v.SecretAccessKey, v.SessionToken
		}), nil
	}

	// oidc role arn
	if o.OIDCProviderARN != "" {
		logutil.Info("aliyun sdk credential", zap.Any("using", "oidc role arn"))
		conf := &credentials.Config{
			Type:              ptrTo("oidc_role_arn"),
			RoleArn:           ptrTo(o.OIDCRoleARN),
			OIDCProviderArn:   ptrTo(o.OIDCProviderARN),
			OIDCTokenFilePath: ptrTo(o.OIDCTokenFilePath),
		}
		if o.RoleSessionName != "" {
			conf.RoleSessionName = &o.RoleSessionName
		}
		if o.ExternalID != "" {
			conf.ExternalId = &o.ExternalID
		}
		var provider credentials.Credential
		provider, err = credentials.NewCredential(conf)
		if err != nil {
			logutil.Error("aliyun credential error", zap.Error(err))
			return
		}
		return aliyunCredentialsProviderFunc(func() (string, string, string) {
			v, err := provider.GetCredential()
			if err != nil {
				logutil.Error("aliyun credential error", zap.Error(err))
				return "", "", ""
			}
			return *v.AccessKeyId, *v.AccessKeySecret, *v.SecurityToken
		}), nil
	}

	return nil, nil
}

type aliyunCredentialsProviderFunc func() (string, string, string)

var _ oss.CredentialsProvider = aliyunCredentialsProviderFunc(nil)

func (a aliyunCredentialsProviderFunc) GetCredentials() oss.Credentials {
	id, secret, token := a()
	return &aliyunCredential{
		KeyID:         id,
		KeySecret:     secret,
		SecurityToken: token,
	}
}

type aliyunCredential struct {
	KeyID         string
	KeySecret     string
	SecurityToken string
}

var _ oss.Credentials = aliyunCredential{}

func (a aliyunCredential) GetAccessKeyID() string {
	return a.KeyID
}

func (a aliyunCredential) GetAccessKeySecret() string {
	return a.KeySecret
}

func (a aliyunCredential) GetSecurityToken() string {
	return a.SecurityToken
}
