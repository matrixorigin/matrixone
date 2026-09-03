// Copyright 2024 Matrix Origin
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
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	gotrace "runtime/trace"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/tencentyun/cos-go-sdk-v5"
	"go.uber.org/zap"
)

type QCloudSDK struct {
	name                 string
	copySourceHost       string
	client               *cos.Client
	copyCredentialDomain objectStorageCopyCredentialDomain
	perfCounterSets      []*perfcounter.CounterSet
	listMaxKeys          int
}

const (
	qcloudMultipartAbortTimeout    = 30 * time.Second
	qcloudMultipartInitTimeout     = 30 * time.Second
	qcloudMultipartInitMaxAttempts = 3
)

func NewQCloudSDK(
	ctx context.Context,
	args ObjectStorageArguments,
	perfCounterSets []*perfcounter.CounterSet,
) (_ *QCloudSDK, err error) {
	defer catch(&err)

	// args
	if err := args.validate(); err != nil {
		return nil, err
	}

	// bucket url
	baseURL, err := url.Parse(fmt.Sprintf(
		"https://%s.cos.%s.myqcloud.com",
		args.Bucket,
		args.Region,
	))
	if err != nil {
		return nil, err
	}

	// credential arguments
	keyID := args.KeyID
	keySecret := args.KeySecret
	sessionToken := args.SessionToken
	if args.shouldLoadDefaultCredentials() {
		keyID = firstNonZero(
			args.KeyID,
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_ACCESS_KEY"),
			os.Getenv("TENCENTCLOUD_SECRETID"),
		)
		keySecret = firstNonZero(
			args.KeySecret,
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			os.Getenv("AWS_SECRET_KEY"),
			os.Getenv("TENCENTCLOUD_SECRETKEY"),
		)
		sessionToken = firstNonZero(
			args.SessionToken,
			os.Getenv("AWS_SESSION_TOKEN"),
			os.Getenv("TENCENTCLOUD_SESSIONTOKEN"),
		)
	}

	// http client
	httpClient := newHTTPClient(args)
	httpClient.Transport = &cos.AuthorizationTransport{
		SecretID:     keyID,
		SecretKey:    keySecret,
		SessionToken: sessionToken,
		Transport:    httpClient.Transport,
	}

	// client
	client := cos.NewClient(
		&cos.BaseURL{BucketURL: baseURL},
		httpClient,
	)

	// Disable COS SDK built-in retry — MatrixOne wraps all operations in
	// its own DoWithRetry, and SDK retry would double the request count
	// and stretch failure latency.
	client.Conf.RetryOpt.Count = 0

	logutil.Info("new object storage",
		zap.Any("sdk", "qcloud"),
		zap.Any("arguments", args),
	)

	if !args.NoBucketValidation {
		// validate bucket
		_, err := DoWithRetryContext(ctx, "cos bucket head", func() (*cos.Response, error) {
			return client.Bucket.Head(ctx, &cos.BucketHeadOptions{})
		}, maxRetryAttemps, IsRetryableError)
		if err != nil {
			return nil, err
		}
	}

	return &QCloudSDK{
		name:           args.Name,
		copySourceHost: baseURL.Host,
		client:         client,
		copyCredentialDomain: newObjectStorageCopyCredentialDomain(
			keyID, keySecret, sessionToken,
		),
		perfCounterSets: perfCounterSets,
	}, nil
}

var _ objectStorageCopier = new(QCloudSDK)

func (a *QCloudSDK) CopyObject(
	ctx context.Context,
	src ObjectStorage,
	srcKey string,
	dstKey string,
) (bool, error) {
	s, ok := src.(*QCloudSDK)
	if !ok || !a.copyCredentialDomain.matches(s.copyCredentialDomain) {
		return false, nil
	}
	_, _, err := a.client.Object.Copy(ctx, dstKey, s.copySourceHost+"/"+srcKey, nil)
	return true, err
}

var _ ObjectStorage = new(QCloudSDK)
var _ ParallelMultipartWriter = new(QCloudSDK)

func (a *QCloudSDK) List(
	ctx context.Context,
	prefix string,
) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(nil, err)
			return
		}

		var cont string

	loop1:
		for {
			result, err := a.listObjects(ctx, prefix, cont)
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
					Name:  prefix,
				}, nil) {
					break loop1
				}
			}

			if !result.IsTruncated {
				break
			}
			cont = result.NextMarker
		}

	}
}

func (a *QCloudSDK) Stat(
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

	header, err := a.statObject(
		ctx,
		key,
	)
	if err != nil {
		return
	}

	if str := header.Get("Content-Length"); str != "" {
		size, err = strconv.ParseInt(str, 10, 64)
		if err != nil {
			return
		}
	}

	return
}

func (a *QCloudSDK) Exists(
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

func (a *QCloudSDK) Write(
	ctx context.Context,
	key string,
	r io.Reader,
	sizeHint *int64,
	expire *time.Time,
) (
	err error,
) {
	defer wrapSizeMismatchErr(&err)

	if sizeHint != nil && *sizeHint < smallObjectThreshold {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		_, err = DoWithRetryContext(ctx, "write", func() (int, error) {
			return 0, a.putObject(
				ctx,
				key,
				bytes.NewReader(data),
				sizeHint,
				expire,
			)
		}, maxRetryAttemps, IsRetryableError)
		if err != nil {
			return err
		}

	} else {
		seeker, ok := r.(io.Seeker)
		if !ok {
			err := a.WriteMultipartParallel(ctx, key, r, sizeHint, &ParallelMultipartOption{
				PartSize:    defaultParallelMultipartPartSize,
				Concurrency: 1,
				Expire:      expire,
			})
			if err != nil {
				return err
			}
			return nil
		}
		offset, err := seeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		_, err = DoWithRetryContext(ctx, "write", func() (int, error) {
			if _, err := seeker.Seek(offset, io.SeekStart); err != nil {
				return 0, err
			}
			return 0, a.putObject(
				ctx,
				key,
				r,
				sizeHint,
				expire,
			)
		}, maxRetryAttemps, IsRetryableError)
		if err != nil {
			return err
		}
	}

	return
}

func (a *QCloudSDK) SupportsParallelMultipart() bool {
	return true
}

func waitQCloudMultipartInitRetry(ctx context.Context, attempt int) error {
	delay := initialRetryInterval
	for i := 0; i < attempt && delay < maxRetryInterval; i++ {
		delay = time.Duration(float64(delay) * retryIntervalFactor)
	}
	if delay > maxRetryInterval {
		delay = maxRetryInterval
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (a *QCloudSDK) initiateMultipartUpload(
	ctx context.Context,
	key string,
	opt *cos.InitiateMultipartUploadOptions,
) (*cos.InitiateMultipartUploadResult, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, qcloudMultipartInitTimeout, context.DeadlineExceeded)
	defer cancel()

	var lastErr error
	for attempt := 0; attempt < qcloudMultipartInitMaxAttempts; attempt++ {
		var wroteRequest atomic.Bool
		attemptCtx := httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
			WroteRequest: func(httptrace.WroteRequestInfo) {
				wroteRequest.Store(true)
			},
		})

		metric.FSMultipartInitAttemptCounter.Inc()
		output, response, createErr := a.client.Object.InitiateMultipartUpload(attemptCtx, key, opt)
		if createErr == nil && (output == nil || output.UploadID == "") {
			createErr = moerr.NewInternalErrorNoCtxf("cos initiate multipart upload returned an empty upload id for key %q", key)
		}
		lastErr = createErr
		if output != nil && output.UploadID != "" {
			if ctxErr := ctx.Err(); ctxErr != nil {
				cleanupCtx, cleanupCancel := context.WithTimeoutCause(
					context.WithoutCancel(ctx),
					qcloudMultipartAbortTimeout,
					context.DeadlineExceeded,
				)
				defer cleanupCancel()
				if err := a.abortMultipartUpload(cleanupCtx, key, output.UploadID); err != nil {
					logutil.Warn("failed to clean up canceled cos multipart init", zap.Error(err))
				} else {
					metric.FSMultipartInitCleanupCounter.Inc()
				}
				return nil, ctxErr
			}
			if attempt > 0 || createErr != nil {
				metric.FSMultipartInitRecoveredCounter.Inc()
			}
			return output, nil
		}
		definitiveFailure := response != nil && response.Response != nil &&
			(response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices)
		commitAmbiguous := !definitiveFailure && (response != nil || wroteRequest.Load())
		if commitAmbiguous {
			metric.FSMultipartInitAmbiguousCounter.Inc()
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if commitAmbiguous {
			// Without an UploadID, no client can distinguish this request's
			// server-side state from another CN's upload. Do not list, claim, or
			// abort candidates here; COS bucket lifecycle must reclaim any orphan.
			logutil.Warn("cos multipart init is commit-ambiguous; bucket lifecycle must abort incomplete uploads",
				zap.Error(createErr))
			return nil, createErr
		}
		if !IsRetryableError(createErr) || attempt+1 >= qcloudMultipartInitMaxAttempts {
			return nil, createErr
		}
		// WroteRequest is emitted by net/http once writing starts, including
		// write failures. Its absence proves this attempt never crossed the
		// transport write boundary. A non-2xx server response also proves the
		// attempt failed, so either outcome is safe to retry.
		if err := waitQCloudMultipartInitRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func (a *QCloudSDK) WriteMultipartParallel(
	ctx context.Context,
	key string,
	r io.Reader,
	sizeHint *int64,
	opt *ParallelMultipartOption,
) (err error) {
	defer wrapSizeMismatchErr(&err)

	options := normalizeParallelOption(opt)
	if sizeHint != nil {
		r = &exactSizeReader{
			R:        r,
			Expected: *sizeHint,
			Key:      key,
		}
		if *sizeHint < minMultipartPartSize {
			return a.Write(ctx, key, r, sizeHint, options.Expire)
		}
		expectedParts := (*sizeHint + options.PartSize - 1) / options.PartSize
		if expectedParts > maxMultipartParts {
			return moerr.NewInternalErrorNoCtxf("too many parts for multipart upload: %d", expectedParts)
		}
	}

	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type partBuffer struct {
		buf    []byte
		n      int
		tokens int64
	}

	releasePartBuffer := func(part *partBuffer) {
		if part == nil {
			return
		}
		releaseParallelUploadBufferBudget(part.tokens)
	}

	readChunk := func() (*partBuffer, error) {
		tokens, err := acquireParallelUploadBufferBudget(ctx, int64(options.PartSize))
		if err != nil {
			return nil, err
		}
		raw := make([]byte, options.PartSize)
		n, err := io.ReadFull(r, raw)
		switch {
		case errors.Is(err, io.EOF):
			releaseParallelUploadBufferBudget(tokens)
			return nil, io.EOF
		case errors.Is(err, io.ErrUnexpectedEOF):
			return &partBuffer{buf: raw, n: n, tokens: tokens}, io.EOF
		case err != nil:
			releaseParallelUploadBufferBudget(tokens)
			return nil, err
		default:
			return &partBuffer{buf: raw, n: n, tokens: tokens}, nil
		}
	}

	firstPart, err := readChunk()
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if firstPart == nil && errors.Is(err, io.EOF) {
		size := int64(0)
		return a.Write(ctx, key, bytes.NewReader(nil), &size, options.Expire)
	}
	if errors.Is(err, io.EOF) && int64(firstPart.n) < minMultipartPartSize {
		data := make([]byte, firstPart.n)
		copy(data, firstPart.buf[:firstPart.n])
		size := int64(firstPart.n)
		releasePartBuffer(firstPart)
		return a.Write(ctx, key, bytes.NewReader(data), &size, options.Expire)
	}

	var expiresHeader string
	if options.Expire != nil {
		expiresHeader = options.Expire.UTC().Format(http.TimeFormat)
	}

	initOpt := &cos.InitiateMultipartUploadOptions{
		ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{
			Expires: expiresHeader,
		},
	}
	output, createErr := a.initiateMultipartUpload(ctx, key, initOpt)
	if createErr != nil {
		releasePartBuffer(firstPart)
		return createErr
	}

	defer func() {
		if err != nil {
			// The upload context is normally canceled on the first part
			// failure, but abort still needs a live context to remove the
			// server-side multipart upload. Bound that detached cleanup so a
			// broken COS endpoint cannot delay the original Write forever.
			abortCtx, abortCancel := context.WithTimeoutCause(
				context.WithoutCancel(parentCtx),
				qcloudMultipartAbortTimeout,
				context.DeadlineExceeded,
			)
			defer abortCancel()
			if abortErr := a.abortMultipartUpload(abortCtx, key, output.UploadID); abortErr != nil {
				logutil.Warn("failed to abort cos multipart upload",
					zap.Error(abortErr))
			}
		}
	}()

	type partJob struct {
		num  int32
		part *partBuffer
	}

	var (
		partNum   int32
		parts     []cos.Object
		partsLock sync.Mutex
		wg        sync.WaitGroup
		errOnce   sync.Once
		firstErr  error
	)

	setErr := func(e error) {
		if e == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = e
			cancel()
		})
	}

	uploadSlots := make(chan struct{}, options.Concurrency)
	startPartUpload := func(job partJob) bool {
		select {
		case uploadSlots <- struct{}{}:
		case <-ctx.Done():
			releasePartBuffer(job.part)
			setErr(ctx.Err())
			return false
		}
		select {
		case getParallelUploadSemaphore() <- struct{}{}:
		case <-ctx.Done():
			<-uploadSlots
			releasePartBuffer(job.part)
			setErr(ctx.Err())
			return false
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				<-getParallelUploadSemaphore()
				<-uploadSlots
			}()
			if ctx.Err() != nil {
				releasePartBuffer(job.part)
				return
			}
			uploadOpt := &cos.ObjectUploadPartOptions{
				ContentLength: int64(job.part.n),
			}
			resp, uploadErr := DoWithRetryContext(ctx, "cos upload part", func() (*cos.Response, error) {
				recordS3PutRequest(ctx, a.perfCounterSets...)
				return a.client.Object.UploadPart(ctx, key, output.UploadID, int(job.num), bytes.NewReader(job.part.buf[:job.part.n]), uploadOpt)
			}, maxRetryAttemps, IsRetryableError)
			if uploadErr != nil {
				setErr(uploadErr)
				releasePartBuffer(job.part)
				return
			}
			recordS3AcceptedBytes(ctx, int64(job.part.n), a.perfCounterSets...)
			etag := ""
			if resp != nil && resp.Header != nil {
				etag = resp.Header.Get("ETag")
			}
			releasePartBuffer(job.part)
			partsLock.Lock()
			parts = append(parts, cos.Object{
				PartNumber: int(job.num),
				ETag:       etag,
			})
			partsLock.Unlock()
		}()
		return true
	}

	sendJob := func(part *partBuffer) bool {
		partNum++
		if partNum > maxMultipartParts {
			setErr(moerr.NewInternalErrorNoCtxf("too many parts for multipart upload: %d", partNum))
			releasePartBuffer(part)
			return false
		}
		job := partJob{
			num:  partNum,
			part: part,
		}
		return startPartUpload(job)
	}

	if sendJob(firstPart) {
		for {
			part, readErr := readChunk()
			if errors.Is(readErr, io.EOF) && part == nil {
				break
			}
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				setErr(readErr)
				releasePartBuffer(part)
				break
			}
			if part == nil || part.n == 0 {
				releasePartBuffer(part)
				break
			}
			if !sendJob(part) {
				break
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
		}
	}

	wg.Wait()

	if firstErr != nil {
		err = firstErr
		return err
	}
	if len(parts) == 0 {
		return nil
	}
	if len(parts) != int(partNum) {
		return moerr.NewInternalErrorNoCtxf("multipart upload incomplete, expect %d parts got %d", partNum, len(parts))
	}

	slices.SortFunc(parts, func(a, b cos.Object) int {
		return cmp.Compare(a.PartNumber, b.PartNumber)
	})

	completeOpt := &cos.CompleteMultipartUploadOptions{
		Parts: parts,
	}
	_, err = DoWithRetryContext(ctx, "cos complete multipart upload", func() (*cos.CompleteMultipartUploadResult, error) {
		res, _, e := a.client.Object.CompleteMultipartUpload(ctx, key, output.UploadID, completeOpt)
		return res, e
	}, maxRetryAttemps, IsRetryableError)
	if err != nil {
		return err
	}

	return nil
}

func (a *QCloudSDK) abortMultipartUpload(
	ctx context.Context,
	key string,
	uploadID string,
) error {
	_, err := DoWithRetryContext(ctx, "cos abort multipart upload", func() (*cos.Response, error) {
		return a.client.Object.AbortMultipartUpload(ctx, key, uploadID)
	}, maxRetryAttemps, IsRetryableError)
	return err
}

func (a *QCloudSDK) Read(
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

func (a *QCloudSDK) Delete(
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

func (a *QCloudSDK) deleteSingle(ctx context.Context, key string) error {
	ctx, span := trace.Start(ctx, "QCloudSDK.deleteSingle")
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

func (a *QCloudSDK) listObjects(ctx context.Context, prefix string, marker string) (*cos.BucketGetResult, error) {
	ctx, task := gotrace.NewTask(ctx, "QCloudSDK.listObjects")
	defer task.End()

	opts := &cos.BucketGetOptions{
		Delimiter: "/",
	}
	if prefix != "" {
		opts.Prefix = prefix
	}
	if marker != "" {
		opts.Marker = marker
	}
	if a.listMaxKeys > 0 {
		opts.MaxKeys = a.listMaxKeys
	}

	return DoWithRetryContext(
		ctx,
		"s3 list objects",
		func() (*cos.BucketGetResult, error) {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.List.Add(1)
			}, a.perfCounterSets...)
			result, _, err := a.client.Bucket.Get(ctx, opts)
			if err != nil {
				return nil, err
			}
			return result, nil
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *QCloudSDK) statObject(ctx context.Context, key string) (http.Header, error) {
	ctx, task := gotrace.NewTask(ctx, "QCloudSDK.statObject")
	defer task.End()

	return DoWithRetryContext(
		ctx,
		"s3 head object",
		func() (http.Header, error) {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.Head.Add(1)
			}, a.perfCounterSets...)
			resp, err := a.client.Object.Head(ctx, key, &cos.ObjectHeadOptions{})
			if err != nil {
				return nil, err
			}
			return resp.Header, nil
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *QCloudSDK) putObject(
	ctx context.Context,
	key string,
	r io.Reader,
	sizeHint *int64,
	expire *time.Time,
) (err error) {
	ctx, task := gotrace.NewTask(ctx, "QCloudSDK.putObject")
	defer task.End()

	recordS3PutRequest(ctx, a.perfCounterSets...)
	var n atomic.Int64
	r = &countingReader{R: r, C: &n}

	// not retryable because Reader may be half consumed
	opts := &cos.ObjectPutOptions{}
	if sizeHint != nil {
		opts.ObjectPutHeaderOptions = &cos.ObjectPutHeaderOptions{
			ContentLength: *sizeHint,
		}
	}
	_, err = a.client.Object.Put(ctx, key, r, opts)
	if err != nil {
		return err
	}
	recordS3AcceptedBytes(ctx, n.Load(), a.perfCounterSets...)
	return nil
}

func (a *QCloudSDK) getObject(ctx context.Context, key string, min *int64, max *int64) (io.ReadCloser, error) {
	ctx, task := gotrace.NewTask(ctx, "QCloudSDK.getObject")
	defer task.End()

	if min == nil {
		min = ptrTo[int64](0)
	}

	return newRetryableReader(
		func(offset int64) (io.ReadCloser, error) {
			var rang string
			if max != nil {
				rang = fmt.Sprintf("bytes=%d-%d", offset, *max)
			} else {
				rang = fmt.Sprintf("bytes=%d-", offset)
			}
			opts := &cos.ObjectGetOptions{
				Range: rang,
			}

			return DoWithRetryContext(
				ctx,
				"s3 get object",
				func() (io.ReadCloser, error) {
					perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
						counter.FileService.S3.Get.Add(1)
					}, a.perfCounterSets...)
					resp, err := a.client.Object.Get(ctx, key, opts)
					if err != nil {
						return nil, err
					}
					return &readCloser{
						r: resp.Body,
						closeFunc: func() error {
							// drain
							io.Copy(io.Discard, resp.Body)
							return resp.Body.Close()
						},
					}, nil
				},
				maxRetryAttemps,
				IsRetryableError,
			)

		},
		*min,
		IsRetryableError,
	)
}

func (a *QCloudSDK) deleteObject(ctx context.Context, key string) (bool, error) {
	ctx, task := gotrace.NewTask(ctx, "QCloudSDK.deleteObject")
	defer task.End()
	return DoWithRetryContext(
		ctx,
		"s3 delete object",
		func() (bool, error) {
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.Delete.Add(1)
			}, a.perfCounterSets...)
			if _, err := a.client.Object.Delete(ctx, key); err != nil {
				return false, err
			}
			return true, nil
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *QCloudSDK) deleteObjects(ctx context.Context, keys ...string) (bool, error) {
	ctx, task := gotrace.NewTask(ctx, "QCloudSDK.deleteObjects")
	defer task.End()
	return DoWithRetryContext(
		ctx,
		"s3 delete objects",
		func() (bool, error) {
			objects := make([]cos.Object, 0, len(keys))
			for _, key := range keys {
				objects = append(objects, cos.Object{
					Key: key,
				})
			}
			perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
				counter.FileService.S3.DeleteMulti.Add(1)
			}, a.perfCounterSets...)
			_, _, err := a.client.Object.DeleteMulti(ctx, &cos.ObjectDeleteMultiOptions{
				Quiet:   true,
				Objects: objects,
			})
			if err != nil {
				return false, err
			}
			return true, nil
		},
		maxRetryAttemps,
		IsRetryableError,
	)
}

func (a *QCloudSDK) is404(err error) bool {
	if err == nil {
		return false
	}
	var resp *cos.ErrorResponse
	if errors.As(err, &resp) {
		return resp.Response.StatusCode == 404
	}
	return false
}
