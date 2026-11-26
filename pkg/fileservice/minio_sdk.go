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
	"io"
	"iter"
	"net/http"
	"net/url"
	"os"
	gotrace "runtime/trace"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

type MinioSDK struct {
	name                   string
	endpoint               string
	supportsConditionalPut bool
	bucket                 string
	// core and client make one attempt per call; the calls on them are retried by
	// DoWithRetryContext.
	core   *minio.Core
	client *minio.Client
	// retryingClient retries within the SDK; it makes the calls DoWithRetryContext does
	// not retry: copies, the bucket check and object reads.
	retryingClient       *minio.Client
	copyCredentialDomain objectStorageCopyCredentialDomain
	perfCounterSets      []*perfcounter.CounterSet
	listMaxKeys          int
}

var _ objectStorageCopier = new(MinioSDK)
var _ objectStorageIdentityReader = new(MinioSDK)

func (a *MinioSDK) CopyObject(
	ctx context.Context,
	src ObjectStorage,
	srcKey string,
	dstKey string,
) (bool, error) {
	s, ok := src.(*MinioSDK)
	if !ok || !strings.EqualFold(a.endpoint, s.endpoint) ||
		!a.copyCredentialDomain.matches(s.copyCredentialDomain) {
		return false, nil
	}
	_, err := a.retryingClient.CopyObject(
		ctx,
		minio.CopyDestOptions{Bucket: a.bucket, Object: dstKey},
		minio.CopySrcOptions{Bucket: s.bucket, Object: srcKey},
	)
	return true, err
}

func NewMinioSDK(
	ctx context.Context,
	args ObjectStorageArguments,
	perfCounterSets []*perfcounter.CounterSet,
) (*MinioSDK, error) {

	if err := args.validate(); err != nil {
		return nil, err
	}

	options := new(minio.Options)

	// credential arguments
	keyID := args.KeyID
	keySecret := args.KeySecret
	sessionToken := args.SessionToken
	if args.shouldLoadDefaultCredentials() {
		keyID = firstNonZero(
			args.KeyID,
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_ACCESS_KEY"),
			os.Getenv("MINIO_ROOT_USER"),
			os.Getenv("MINIO_ACCESS_KEY"),
		)
		keySecret = firstNonZero(
			args.KeySecret,
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			os.Getenv("AWS_SECRET_KEY"),
			os.Getenv("MINIO_ROOT_PASSWORD"),
			os.Getenv("MINIO_SECRET_KEY"),
		)
		sessionToken = firstNonZero(
			args.SessionToken,
			os.Getenv("AWS_SESSION_TOKEN"),
		)
	}

	// credentials providers
	var credentialProviders []credentials.Provider

	if keyID != "" && keySecret != "" {
		// static
		credentialProviders = append(credentialProviders, &credentials.Static{
			Value: credentials.Value{
				AccessKeyID:     keyID,
				SecretAccessKey: keySecret,
				SessionToken:    sessionToken,
				SignerType:      credentials.SignatureV2,
			},
		})
		credentialProviders = append(credentialProviders, &credentials.Static{
			Value: credentials.Value{
				AccessKeyID:     keyID,
				SecretAccessKey: keySecret,
				SessionToken:    sessionToken,
				SignerType:      credentials.SignatureV4,
			},
		})
		credentialProviders = append(credentialProviders, &credentials.Static{
			Value: credentials.Value{
				AccessKeyID:     keyID,
				SecretAccessKey: keySecret,
				SessionToken:    sessionToken,
				SignerType:      credentials.SignatureDefault,
			},
		})

	}

	if args.RoleARN != "" {
		// assume role
		credentialProviders = append(credentialProviders, &credentials.STSAssumeRole{
			Options: credentials.STSAssumeRoleOptions{
				AccessKey:       keyID,
				SecretKey:       keySecret,
				RoleARN:         args.RoleARN,
				RoleSessionName: args.ExternalID,
			},
		})
	}

	// special treatments for 天翼云
	if strings.Contains(args.Endpoint, "ctyunapi.cn") {
		if keyID == "" {
			// try to fetch one
			creds := credentials.NewChainCredentials(credentialProviders)
			value, err := creds.Get()
			if err != nil {
				return nil, err
			}
			keyID = value.AccessKeyID
			keySecret = value.SecretAccessKey
			sessionToken = value.SessionToken
		}
		credentialProviders = []credentials.Provider{
			&credentials.Static{
				Value: credentials.Value{
					AccessKeyID:     keyID,
					SecretAccessKey: keySecret,
					SessionToken:    sessionToken,
					SignerType:      credentials.SignatureV2,
				},
			},
		}
	}

	if args.callerCredentialsOnly && args.RoleARN != "" {
		// A requested role must never fall back to static or anonymous credentials.
		client := newHTTPClient(args)
		client.Transport = sqlMinioSTSTransport{ctx: ctx, next: client.Transport}
		provider := &credentials.STSAssumeRole{
			Client: client, STSEndpoint: args.Endpoint,
			Options: credentials.STSAssumeRoleOptions{AccessKey: keyID, SecretKey: keySecret, SessionToken: sessionToken, RoleARN: args.RoleARN, RoleSessionName: args.RoleSessionName, ExternalID: args.ExternalID, Location: args.Region},
		}
		options.Creds = credentials.New(provider)
		if _, err := options.Creds.Get(); err != nil {
			return nil, err
		}
	} else {
		options.Creds = credentials.NewChainCredentials(credentialProviders)
	}

	// region
	if args.Region != "" {
		options.Region = args.Region
	}

	// bucket lookup style
	if strings.Contains(args.Endpoint, "myqcloud") ||
		strings.Contains(args.Endpoint, "aliyuncs") {
		// 腾讯云，阿里云默认使用virtual host style
		options.BucketLookup = minio.BucketLookupDNS
	}

	// transport
	options.Transport = newHTTPClient(args).Transport

	// endpoint
	isSecure, err := minioValidateEndpoint(&args)
	if err != nil {
		return nil, err
	}
	options.Secure = isSecure

	retryingClient, err := minio.New(args.Endpoint, options)
	if err != nil {
		return nil, err
	}
	options.MaxRetries = 1
	client, err := minio.New(args.Endpoint, options)
	if err != nil {
		return nil, err
	}
	core, err := minio.NewCore(args.Endpoint, options)
	if err != nil {
		return nil, err
	}

	logutil.Info("new object storage",
		zap.Any("sdk", "minio"),
		zap.Any("arguments", args),
	)

	if !args.NoBucketValidation {
		// validate
		ok, err := retryingClient.BucketExists(ctx, args.Bucket)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, moerr.NewInternalErrorNoCtxf(
				"bad s3 config, no such bucket or no permissions: %v",
				args.Bucket,
			)
		}
	}

	return &MinioSDK{
		name:                   args.Name,
		endpoint:               args.Endpoint,
		supportsConditionalPut: args.IsMinio && !minioEndpointIsKnownUnsupported(args.Endpoint),
		bucket:                 args.Bucket,
		client:                 client,
		core:                   core,
		retryingClient:         retryingClient,
		copyCredentialDomain: newObjectStorageCopyCredentialDomain(
			keyID, keySecret, sessionToken, args.RoleARN, args.ExternalID,
		),
		perfCounterSets: perfCounterSets,
	}, nil
}

// MinIO's STS provider creates requests without a context. SQL dynamic services
// belong to one operation, so initial credentials and refresh borrow its lifetime.
// This transport is used only for STS, not ordinary object requests or native services.
type sqlMinioSTSTransport struct {
	ctx  context.Context
	next http.RoundTripper
}

func (t sqlMinioSTSTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.next.RoundTrip(req.WithContext(t.ctx))
}

func minioEndpointIsKnownUnsupported(endpoint string) bool {
	endpoint = strings.ToLower(endpoint)
	return strings.Contains(endpoint, "ctyunapi.cn") ||
		strings.Contains(endpoint, "myqcloud.com") ||
		strings.Contains(endpoint, "aliyuncs.com")
}

var _ ObjectStorage = new(MinioSDK)

func (a *MinioSDK) List(
	ctx context.Context,
	prefix string,
) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(nil, err)
			return
		}

		var marker string

	loop1:
		for {
			result, err := a.listObjects(ctx, prefix, marker)
			if err != nil {
				yield(nil, err)
				return
			}

			for _, obj := range result.Contents {
				if !yield(&DirEntry{
					Name: obj.Key,
					Size: obj.Size,
				}, nil) {
					break loop1
				}
			}

			for _, prefix := range result.CommonPrefixes {
				if !yield(&DirEntry{
					IsDir: true,
					Name:  prefix.Prefix,
				}, nil) {
					break loop1
				}
			}

			if !result.IsTruncated {
				break
			}
			marker = result.NextMarker
		}

	}
}

func (a *MinioSDK) Stat(
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

	size = info.Size

	return
}

func (a *MinioSDK) StatObjectIdentity(ctx context.Context, key string) (ObjectIdentity, error) {
	info, err := a.statObject(ctx, key)
	if err != nil {
		if a.is404(err) {
			return ObjectIdentity{}, moerr.NewFileNotFoundNoCtx(key)
		}
		return ObjectIdentity{}, err
	}
	identity := ObjectIdentity{
		VersionID: info.VersionID, ETag: info.ETag, Size: info.Size, LastModified: info.LastModified,
	}
	return identity, identity.Validate()
}

func (a *MinioSDK) ReadObjectWithIdentity(
	ctx context.Context,
	key string,
	min *int64,
	max *int64,
	expected ObjectIdentity,
) (io.ReadCloser, error) {
	if err := expected.Validate(); err != nil {
		return nil, err
	}
	options := minio.GetObjectOptions{}
	if expected.VersionID != "" {
		options.VersionID = expected.VersionID
	} else if err := options.SetMatchETag(expected.ETag); err != nil {
		return nil, err
	}
	r, err := a.getObjectWithOptions(ctx, key, min, max, options)
	if err != nil {
		return nil, mapMinioConditionalReadError(err)
	}
	// MinIO defers the GET until the first operation on Object.
	if _, err = r.Read(nil); err != nil {
		r.Close()
		return nil, mapMinioConditionalReadError(err)
	}
	return mapReadCloserErrors(r, mapMinioConditionalReadError), nil
}

func mapMinioConditionalReadError(err error) error {
	response := minio.ToErrorResponse(err)
	if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusPreconditionFailed {
		return errors.Join(ErrObjectChanged, moerr.NewInternalErrorNoCtx("conditional S3-compatible read failed"))
	}
	return err
}

func (a *MinioSDK) Exists(
	ctx context.Context,
	key string,
) (
	bool,
	error,
) {

	return objectExists(ctx, key, a.statObject, a.is404)
}

func (a *MinioSDK) Write(
	ctx context.Context,
	key string,
	r io.Reader,
	sizeHint *int64,
	expire *time.Time,
) (
	err error,
) {
	defer wrapSizeMismatchErr(&err)

	if sizeHint != nil && (*sizeHint <= smallObjectThreshold || exclusiveWrite(ctx)) {
		body, size, err := bodyOf(r)
		if err != nil {
			return err
		}
		if size != *sizeHint {
			return moerr.NewSizeNotMatchNoCtx(key)
		}
		_, err = DoWithRetryContext(ctx, "write", func() (minio.UploadInfo, error) {
			return a.putObject(
				ctx,
				key,
				io.NewSectionReader(body, 0, size),
				sizeHint,
				expire,
			)
		}, maxRetryAttemps, minioRetryable())
		return err
	}
	return writeParts(ctx, storageParts{a}, key, r, sizeHint, &ParallelMultipartOption{Concurrency: 1, Expire: expire})
}

func (a *MinioSDK) Read(
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

	r, err = a.getObject(
		ctx,
		key,
		min,
		max,
	)
	if err != nil {
		return nil, err
	}
	// eager read to expose file not found error
	_, err = r.Read(nil)
	if err != nil {
		r.Close()
		return nil, err
	}
	return r, nil
}

func (a *MinioSDK) Delete(
	ctx context.Context,
	keys ...string,
) (
	err error,
) {

	return deleteObjectKeys(ctx, keys, 0, a.deleteSingle, a.deleteObjects)
}

func (a *MinioSDK) deleteSingle(ctx context.Context, key string) error {
	ctx, span := trace.Start(ctx, "MinioSDK.deleteSingle")
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

func (a *MinioSDK) listObjects(ctx context.Context, prefix string, marker string) (minio.ListBucketResult, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.listObjects")
	defer task.End()
	return DoWithRetryContext(
		ctx,
		"s3 list objects",
		func() (minio.ListBucketResult, error) {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.List.Add(1)
			}, a.perfCounterSets...)
			return a.core.ListObjects(
				a.bucket,
				prefix,
				marker,
				"/",
				a.listMaxKeys,
			)
		},
		maxRetryAttemps,
		minioRetryable(),
	)
}

func (a *MinioSDK) statObject(ctx context.Context, key string) (minio.ObjectInfo, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.statObject")
	defer task.End()
	return DoWithRetryContext(
		ctx,
		"s3 head object",
		func() (minio.ObjectInfo, error) {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.Head.Add(1)
			}, a.perfCounterSets...)
			return a.client.StatObject(
				ctx,
				a.bucket,
				key,
				minio.StatObjectOptions{},
			)
		},
		maxRetryAttemps,
		minioRetryable(),
	)
}

func (a *MinioSDK) putObject(
	ctx context.Context,
	key string,
	r io.Reader,
	sizeHint *int64,
	expire *time.Time,
) (minio.UploadInfo, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.putObject")
	defer task.End()
	// not retryable because Reader may be half consumed
	//TODO set expire
	recordS3PutRequest(ctx, a.perfCounterSets...)
	size := int64(-1)
	if sizeHint != nil {
		size = *sizeHint
	}
	opts := minio.PutObjectOptions{DisableMultipart: sizeHint != nil}
	if expire != nil {
		opts.Expires = *expire
	}
	if exclusiveWrite(ctx) {
		opts.DisableMultipart = true
		opts.SetMatchETagExcept("*")
	}
	info, err := a.client.PutObject(
		ctx,
		a.bucket,
		key,
		r,
		size,
		opts,
	)
	if err == nil {
		recordS3AcceptedBytes(ctx, info.Size, a.perfCounterSets...)
	}
	if exclusiveWrite(ctx) && minio.ToErrorResponse(err).StatusCode == 412 {
		err = moerr.NewFileAlreadyExistsNoCtx(key)
	}
	return info, err
}

var _ multipartStorage = new(MinioSDK)

func (a *MinioSDK) createMultipart(ctx context.Context, key string, expire *time.Time) (string, error) {
	opts := minio.PutObjectOptions{}
	if expire != nil {
		opts.Expires = *expire
	}
	return DoWithRetryContext(ctx, "create multipart upload", func() (string, error) {
		return a.core.NewMultipartUpload(ctx, a.bucket, key, opts)
	}, maxRetryAttemps, minioRetryable())
}

func (a *MinioSDK) uploadPart(ctx context.Context, key, uploadID string, num int32, body io.ReaderAt, size int64) (string, error) {
	part, err := DoWithRetryContext(ctx, "upload part", func() (minio.ObjectPart, error) {
		recordS3PutRequest(ctx, a.perfCounterSets...)
		return a.core.PutObjectPart(ctx, a.bucket, key, uploadID, int(num), io.NewSectionReader(body, 0, size), size, minio.PutObjectPartOptions{})
	}, maxRetryAttemps, minioRetryable())
	if err != nil {
		return "", err
	}
	recordS3AcceptedBytes(ctx, size, a.perfCounterSets...)
	return part.ETag, nil
}

func (a *MinioSDK) completeMultipart(ctx context.Context, key, uploadID string, parts []multipartPart) error {
	completed := make([]minio.CompletePart, len(parts))
	for i, p := range parts {
		completed[i] = minio.CompletePart{PartNumber: int(p.num), ETag: p.etag}
	}
	_, err := DoWithRetryContext(ctx, "complete multipart upload", func() (minio.UploadInfo, error) {
		return a.core.CompleteMultipartUpload(ctx, a.bucket, key, uploadID, completed, minio.PutObjectOptions{})
	}, maxRetryAttemps, minioRetryable())
	return err
}

func (a *MinioSDK) abortMultipart(ctx context.Context, key, uploadID string) error {
	_, err := DoWithRetryContext(ctx, "abort multipart upload", func() (bool, error) {
		return true, a.core.AbortMultipartUpload(ctx, a.bucket, key, uploadID)
	}, maxRetryAttemps, minioRetryable())
	return err
}

func (a *MinioSDK) getObject(ctx context.Context, key string, min *int64, max *int64) (io.ReadCloser, error) {
	return a.getObjectWithOptions(ctx, key, min, max, minio.GetObjectOptions{})
}

func (a *MinioSDK) getObjectWithOptions(
	ctx context.Context,
	key string,
	min *int64,
	max *int64,
	options minio.GetObjectOptions,
) (io.ReadCloser, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.getObject")
	defer task.End()
	if min == nil {
		min = ptrTo[int64](0)
	}
	if max != nil && *max <= *min {
		return nil, moerr.NewEmptyRangeNoCtx(key)
	}
	r, err := newRetryableReader(
		func(offset int64) (io.ReadCloser, error) {
			if max != nil {
				if err := options.SetRange(offset, *max-1); err != nil {
					return nil, err
				}
			}
			obj, err := DoWithRetryContext(
				ctx,
				"s3 get object",
				func() (obj *minio.Object, err error) {
					perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
						counter.FileService.S3.Get.Add(1)
					}, a.perfCounterSets...)
					return a.retryingClient.GetObject(ctx, a.bucket, key, options)
				},
				maxRetryAttemps,
				IsRetryableError,
			)
			if err != nil {
				return nil, err
			}
			if max != nil {
				// Seek would replace the bounded Range with an open-ended one.
				// Close the bounded response directly, without draining unread data.
				return obj, nil
			}
			if offset > 0 {
				if _, err := obj.Seek(offset, 0); err != nil {
					obj.Close()
					return nil, err
				}
			}
			return &readCloser{
				r: obj,
				closeFunc: func() error {
					// drain
					io.Copy(io.Discard, obj)
					return obj.Close()
				},
			}, nil
		},
		*min,
		IsRetryableError,
	)
	if err != nil {
		return nil, err
	}
	if max != nil {
		return &readCloser{r: io.LimitReader(r, *max-*min), closeFunc: r.Close}, nil
	}
	return r, nil
}

func (a *MinioSDK) deleteObject(ctx context.Context, key string) (any, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.deleteObject")
	defer task.End()
	return DoWithRetryContext(
		ctx,
		"s3 delete object",
		func() (any, error) {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.Delete.Add(1)
			}, a.perfCounterSets...)
			if err := a.client.RemoveObject(ctx, a.bucket, key, minio.RemoveObjectOptions{}); err != nil {
				return nil, err
			}
			return nil, nil
		},
		maxRetryAttemps,
		minioRetryable(),
	)
}

func (a *MinioSDK) deleteObjects(ctx context.Context, keys ...string) (any, error) {
	ctx, task := gotrace.NewTask(ctx, "MinioSDK.deleteObjects")
	defer task.End()
	return DoWithRetryContext(
		ctx,
		"s3 delete objects",
		func() (any, error) {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.DeleteMulti.Add(1)
			}, a.perfCounterSets...)
			objsCh := make(chan minio.ObjectInfo)
			errCh := a.client.RemoveObjects(ctx, a.bucket, objsCh, minio.RemoveObjectsOptions{})
			for _, key := range keys {
				objsCh <- minio.ObjectInfo{
					Key: key,
				}
			}
			close(objsCh)
			for err := range errCh {
				return nil, err.Err
			}
			return nil, nil
		},
		maxRetryAttemps,
		minioRetryable(),
	)
}

// minioRetryableStatus and minioRetryableCodes are the response statuses and error codes
// the MinIO SDK retries.
var (
	minioRetryableStatus = map[int]bool{
		http.StatusRequestTimeout: true, http.StatusTooManyRequests: true, 499: true,
		http.StatusInternalServerError: true, http.StatusBadGateway: true,
		http.StatusServiceUnavailable: true, http.StatusGatewayTimeout: true, 520: true,
	}
	minioRetryableCodes = map[string]bool{
		"RequestError": true, "RequestTimeout": true, "Throttling": true, "ThrottlingException": true,
		"RequestLimitExceeded": true, "RequestThrottled": true, "InternalError": true,
		"ExpiredToken": true, "ExpiredTokenException": true,
		"SlowDown": true, "SlowDownWrite": true, "SlowDownRead": true,
	}
)

// minioRetryable returns the retry check of one call: an error IsRetryableError retries
// is retried; an error whose response has a status or an error code the MinIO SDK retries
// is retried as often as the SDK retries it.
func minioRetryable() func(error) bool {
	sdkRetries := minio.MaxRetry - 1
	return func(err error) bool {
		if IsRetryableError(err) {
			return true
		}
		resp := minio.ToErrorResponse(err)
		if sdkRetries > 0 && (minioRetryableStatus[resp.StatusCode] || minioRetryableCodes[resp.Code]) {
			sdkRetries--
			return true
		}
		return false
	}
}

func (a *MinioSDK) is404(err error) bool {
	if err == nil {
		return false
	}
	resp := minio.ToErrorResponse(err)
	return resp.Code == "NoSuchKey" || resp.StatusCode == http.StatusNotFound
}

func minioValidateEndpoint(args *ObjectStorageArguments) (isSecure bool, err error) {
	if args.Endpoint == "" {
		return false, nil
	}

	endpointURL, err := url.Parse(args.Endpoint)
	if err != nil {
		return false, err
	}
	isSecure = endpointURL.Scheme == "https"
	endpointURL.Scheme = ""
	args.Endpoint = endpointURL.String()
	args.Endpoint = strings.TrimLeft(args.Endpoint, "/")

	return
}
