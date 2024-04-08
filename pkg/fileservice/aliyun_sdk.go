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
	gotrace "runtime/trace"
	"strconv"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/sts"
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
	perfCounterSets []*perfcounter.CounterSet
	listMaxKeys     int
}

var (
	aliyunCredentialExpireDuration = time.Minute * 30
)

func NewAliyunSDK(
	ctx context.Context,
	args ObjectStorageArguments,
	perfCounterSets []*perfcounter.CounterSet,
) (_ *AliyunSDK, err error) {
	defer catch(&err)

	if err := args.validate(); err != nil {
		return nil, err
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

	logutil.Info("new object storage",
		zap.Any("sdk", "aliyun"),
		zap.Any("arguments", args),
	)

	if !args.NoBucketValidation {
		// validate bucket
		_, err := client.GetBucketInfo(args.Bucket)
		if err != nil {
			return nil, err
		}
	}

	bucket, err := client.Bucket(args.Bucket)
	if err != nil {
		return nil, err
	}

	return &AliyunSDK{
		name:            args.Name,
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

	defer func() {
		if a.is404(err) {
			err = moerr.NewFileNotFoundNoCtx(key)
		}
	}()

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

	err = a.putObject(
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
) (err error) {
	defer catch(&err)
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
	)
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

func (a *AliyunSDK) deleteObject(ctx context.Context, key string) (bool, error) {
	ctx, task := gotrace.NewTask(ctx, "AliyunSDK.deleteObject")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Delete.Add(1)
	}, a.perfCounterSets...)
	return doWithRetry[bool](
		"s3 delete object",
		func() (bool, error) {
			if err := a.bucket.DeleteObject(
				key,
				oss.WithContext(ctx),
			); err != nil {
				return false, err
			}
			return true, nil
		},
		maxRetryAttemps,
		isRetryableError,
	)
}

func (a *AliyunSDK) deleteObjects(ctx context.Context, keys ...string) (bool, error) {
	ctx, task := gotrace.NewTask(ctx, "AliyunSDK.deleteObjects")
	defer task.End()
	t0 := time.Now()
	defer func() {
		FSProfileHandler.AddSample(time.Since(t0))
	}()
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.DeleteMulti.Add(1)
	}, a.perfCounterSets...)
	return doWithRetry[bool](
		"s3 delete objects",
		func() (bool, error) {
			_, err := a.bucket.DeleteObjects(
				keys,
				oss.WithContext(ctx),
			)
			if err != nil {
				return false, err
			}
			return true, nil
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
		// chain assume role provider
		if o.RoleARN != "" {
			logutil.Info("with role arn")
			upstream := ret
			ret = &aliyunAssumeRoleCredentialsProvider{
				args:     o,
				upstream: upstream,
			}
		}
	}()

	config := new(credentials.Config)

	// access key
	if o.KeyID != "" && o.KeySecret != "" {

		if o.SecurityToken != "" {
			// sts
			config.SetType("sts")
			config.SetAccessKeyId(o.KeyID)
			config.SetAccessKeySecret(o.KeySecret)
			config.SetSecurityToken(o.SecurityToken)

		} else {
			// static
			config.SetType("access_key")
			config.SetAccessKeyId(o.KeyID)
			config.SetAccessKeySecret(o.KeySecret)
		}

	} else if o.RAMRole != "" {
		// ecs ram role
		config.SetType("ecs_ram_role")
		config.SetRoleName(o.RAMRole)

	} else if o.BearerToken != "" {
		// bearer token
		config.SetType("bearer")
		config.SetBearerToken(o.BearerToken)
	}

	if config.Type == nil {

		if !o.shouldLoadDefaultCredentials() {
			return nil, moerr.NewInvalidInputNoCtx(
				"no valid credentials",
			)
		}

		// check aws env
		awsCredentials := awscredentials.NewEnvCredentials()
		_, err = awsCredentials.Get()
		if err == nil {
			logutil.Info("using aws env credentials")
			return aliyunCredentialsProviderFunc(func() (string, string, string) {
				v, err := awsCredentials.Get()
				if err != nil {
					throw(err)
				}
				return v.AccessKeyID, v.SecretAccessKey, v.SessionToken
			}), nil
		}

		// default chain
		logutil.Info("using default credential chain")
		provider, err := credentials.NewCredential(nil)
		if err != nil {
			return nil, err
		}
		ret := toOSSCredentialProvider(provider)
		return ret, nil
	}

	// from config
	logutil.Info("credential from config",
		zap.Any("type", *config.Type),
	)
	provider, err := credentials.NewCredential(config)
	if err != nil {
		return nil, err
	}
	return toOSSCredentialProvider(provider), nil

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

func toOSSCredentialProvider(
	provider credentials.Credential,
) oss.CredentialsProvider {
	return &ossCredentialProvider{
		upstream: provider,
	}
}

type ossCredentialProvider struct {
	upstream credentials.Credential
}

var _ oss.CredentialsProvider = new(ossCredentialProvider)

func (o *ossCredentialProvider) GetCredentials() oss.Credentials {
	return o
}

var _ oss.Credentials = new(ossCredentialProvider)

func (o *ossCredentialProvider) GetAccessKeyID() string {
	ret, err := o.upstream.GetAccessKeyId()
	if err != nil {
		throw(err)
	}
	return *ret
}

func (o *ossCredentialProvider) GetAccessKeySecret() string {
	ret, err := o.upstream.GetAccessKeySecret()
	if err != nil {
		throw(err)
	}
	return *ret
}

func (o *ossCredentialProvider) GetSecurityToken() string {
	ret, err := o.upstream.GetSecurityToken()
	if err != nil {
		throw(err)
	}
	return *ret
}

type aliyunAssumeRoleCredentialsProvider struct {
	args       ObjectStorageArguments
	upstream   oss.CredentialsProvider
	credential aliyunCredential
	validUntil time.Time
}

var _ oss.CredentialsProvider = new(aliyunAssumeRoleCredentialsProvider)

func (a *aliyunAssumeRoleCredentialsProvider) GetCredentials() oss.Credentials {
	if err := a.refresh(); err != nil {
		throw(err)
	}
	return a.credential
}

func (a *aliyunAssumeRoleCredentialsProvider) refresh() error {
	if time.Until(a.validUntil) > time.Minute*5 {
		return nil
	}

	credential := a.upstream.GetCredentials()
	var client *sts.Client
	var err error
	if securityToken := credential.GetSecurityToken(); securityToken != "" {
		client, err = sts.NewClientWithStsToken(
			a.args.Region,
			credential.GetAccessKeyID(),
			credential.GetAccessKeySecret(),
			securityToken,
		)
	} else {
		client, err = sts.NewClientWithAccessKey(
			a.args.Region,
			credential.GetAccessKeyID(),
			credential.GetAccessKeySecret(),
		)
	}
	if err != nil {
		return err
	}

	req := sts.CreateAssumeRoleRequest()
	req.Scheme = "https"
	req.RoleSessionName = a.args.RoleSessionName
	req.DurationSeconds = requests.NewInteger(int(aliyunCredentialExpireDuration / time.Second))
	req.ExternalId = a.args.ExternalID
	req.RoleArn = a.args.RoleARN

	resp, err := client.AssumeRole(req)
	if err != nil {
		return err
	}

	expire, err := time.Parse(time.RFC3339, resp.Credentials.Expiration)
	if err != nil {
		logutil.Warn("bad expire time from response",
			zap.Any("time", resp.Credentials.Expiration),
		)
		a.validUntil = time.Now().Add(aliyunCredentialExpireDuration)
	} else {
		a.validUntil = expire
	}
	a.credential.KeyID = resp.Credentials.AccessKeyId
	a.credential.KeySecret = resp.Credentials.AccessKeySecret
	a.credential.SecurityToken = resp.Credentials.SecurityToken

	return nil
}
