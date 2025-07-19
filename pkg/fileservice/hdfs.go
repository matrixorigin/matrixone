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
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/colinmarc/hdfs/v2"
	krb "github.com/jcmturner/gokrb5/v8/client"
	krbconfig "github.com/jcmturner/gokrb5/v8/config"
	krbkeytab "github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"go.uber.org/zap"
)

type HDFS struct {
	name            string
	client          *hdfs.Client
	rootPath        string
	perfCounterSets []*perfcounter.CounterSet
}

func NewHDFS(
	ctx context.Context,
	args ObjectStorageArguments,
	perfCounterSets []*perfcounter.CounterSet,
) (*HDFS, error) {

	if err := args.validate(); err != nil {
		return nil, err
	}

	if !strings.HasPrefix(args.Endpoint, "hdfs://") {
		args.Endpoint = "hdfs://" + args.Endpoint
	}
	u, err := url.Parse(args.Endpoint)
	if err != nil {
		return nil, err
	}

	args.User = firstNonZero(
		args.User,
		u.Query().Get("user"),
		"hadoop",
	)

	kerberosClient, err := args.getKerberosClient()
	if err != nil {
		return nil, err
	}

	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: []string{
			u.Host,
		},
		User:                         args.User,
		NamenodeDialFunc:             httpDialer.DialContext,
		DatanodeDialFunc:             httpDialer.DialContext,
		KerberosServicePrincipleName: args.KerberosServicePrincipleName,
		KerberosClient:               kerberosClient,
	})
	if err != nil {
		return nil, err
	}

	rootPath := args.Bucket
	if !strings.HasPrefix(rootPath, "/") {
		rootPath = "/" + rootPath
	}

	ret := &HDFS{
		name:            args.Name,
		client:          client,
		rootPath:        rootPath,
		perfCounterSets: perfCounterSets,
	}

	if !args.NoBucketValidation && ret.rootPath != "" {
		if err := client.MkdirAll(ret.rootPath, 0755); err != nil {
			return nil, err
		}

		stat, err := client.Stat(ret.rootPath)
		if err != nil {
			return nil, err
		}
		if stat.Mode().Perm()&0444 == 0 {
			return nil, moerr.NewBadConfigNoCtx(fmt.Sprintf("path not readable: %s", ret.rootPath))
		}
	}

	logutil.Info("new object storage",
		zap.Any("sdk", "hdfs"),
		zap.Any("arguments", args),
	)

	return ret, nil
}

var _ ObjectStorage = new(HDFS)

func (h *HDFS) Delete(ctx context.Context, keys ...string) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Delete.Add(1)
	}, h.perfCounterSets...)

	for _, key := range keys {
		_, err := DoWithRetry("HDFS: delete", func() (bool, error) {
			return true, h.client.Remove(h.keyToPath(key))
		}, maxRetryAttemps, IsRetryableError)
		if err != nil {
			logutil.Warn("HDFS: delete file", zap.Error(err))
		}
	}

	return nil
}

func (h *HDFS) keyToPath(key string) string {
	ret := path.Join(h.rootPath, key)
	// use absolute path
	if !strings.HasPrefix(ret, "/") {
		ret = "/" + ret
	}
	return ret
}

func (h *HDFS) createTemp() (*hdfs.FileWriter, string, error) {
	for {
		randomBytes := make([]byte, 26)
		rand.Read(randomBytes)
		name := h.keyToPath(
			base32.HexEncoding.WithPadding(base32.NoPadding).EncodeToString(randomBytes),
		)
		_, err := DoWithRetry("HDFS: stat", func() (os.FileInfo, error) {
			return h.client.Stat(name)
		}, maxRetryAttemps, IsRetryableError)
		if err == nil {
			// existed
			continue
		}
		file, err := DoWithRetry("HDFS: create", func() (*hdfs.FileWriter, error) {
			return h.client.Create(name)
		}, maxRetryAttemps, IsRetryableError)
		if err != nil {
			return nil, "", err
		}
		return file, name, nil
	}
}

func (h *HDFS) Exists(ctx context.Context, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Head.Add(1)
	}, h.perfCounterSets...)

	_, err := DoWithRetry("HDFS: stat", func() (os.FileInfo, error) {
		return h.client.Stat(h.keyToPath(key))
	}, maxRetryAttemps, IsRetryableError)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (h *HDFS) List(ctx context.Context, prefix string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(nil, err)
			return
		}

		perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
			counter.FileService.S3.List.Add(1)
		}, h.perfCounterSets...)

		dir, prefix := path.Split(prefix)

		f, err := DoWithRetry("HDFS: open", func() (*hdfs.FileReader, error) {
			return h.client.Open(h.keyToPath(dir))
		}, maxRetryAttemps, IsRetryableError)
		if err != nil {
			if os.IsNotExist(err) {
				return
			}
			yield(nil, err)
			return
		}
		defer f.Close()

	read:
		for {
			infos, err := f.Readdir(256)

			for _, info := range infos {
				name := info.Name()
				if strings.HasSuffix(name, ".mofstemp") {
					continue
				}
				if !strings.HasPrefix(name, prefix) {
					continue
				}
				if !yield(&DirEntry{
					IsDir: info.IsDir(),
					Name:  path.Join(dir, name),
					Size:  info.Size(),
				}, nil) {
					break read
				}
			}

			if err != nil {
				if err == io.EOF {
					break read
				}
				yield(nil, err)
				return

			} else if len(infos) == 0 {
				// no error, and no infos
				break read
			}

		}

	}
}

func (h *HDFS) Read(ctx context.Context, key string, min *int64, max *int64) (r io.ReadCloser, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Get.Add(1)
	}, h.perfCounterSets...)

	f, err := DoWithRetry("HDFS: open", func() (*hdfs.FileReader, error) {
		return h.client.Open(h.keyToPath(key))
	}, maxRetryAttemps, IsRetryableError)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, moerr.NewFileNotFoundNoCtx(key)
		}
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, f.Close())
		}
	}()

	var reader io.Reader
	reader = f

	if min != nil {
		pos, err := f.Seek(*min, io.SeekStart)
		if err != nil {
			return nil, err
		}
		if pos != *min {
			return nil, moerr.NewEmptyRangeNoCtx(key)
		}
	}

	if max != nil {
		limit := *max
		if min != nil {
			limit -= *min
		}
		if limit <= 0 {
			return nil, moerr.NewEmptyRangeNoCtx(key)
		}
		reader = io.LimitReader(reader, limit)
	}

	return &readCloser{
		r:         reader,
		closeFunc: f.Close,
	}, nil
}

func (h *HDFS) Stat(ctx context.Context, key string) (size int64, err error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Head.Add(1)
	}, h.perfCounterSets...)

	stat, err := DoWithRetry("HDFS: stat", func() (os.FileInfo, error) {
		return h.client.Stat(h.keyToPath(key))
	}, maxRetryAttemps, IsRetryableError)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, moerr.NewFileNotFoundNoCtx(key)
		}
		return 0, err
	}

	size = stat.Size()

	return
}

func (h *HDFS) Write(ctx context.Context, key string, r io.Reader, sizeHint *int64, expire *time.Time) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Put.Add(1)
	}, h.perfCounterSets...)

	tempFile, tempFilePath, err := h.createTemp()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tempFile.Close()
			_ = h.client.Remove(tempFilePath)
		}
	}()

	n, err := io.Copy(
		tempFile,
		r,
	)
	if err != nil {
		return err
	}

	if sizeHint != nil && n != *sizeHint {
		return moerr.NewSizeNotMatchNoCtx(key)
	}

	_, err = DoWithRetry("HDFS: FileWriter.Close", func() (bool, error) {
		// may retrun 'replication in progress' error, retry until OK
		return true, tempFile.Close()
	}, maxRetryAttemps, IsRetryableError)
	if err != nil {
		return err
	}

	filePath := h.keyToPath(key)
	_, err = DoWithRetry("HDFS: MkdirAll", func() (bool, error) {
		return true, h.client.MkdirAll(path.Dir(filePath), 0755)
	}, maxRetryAttemps, IsRetryableError)
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}

	_, err = DoWithRetry("HDFS: Rename", func() (bool, error) {
		return true, h.client.Rename(tempFilePath, filePath)
	}, maxRetryAttemps, IsRetryableError)
	if err != nil {
		return err
	}

	return nil
}

func (o *ObjectStorageArguments) getKerberosClient() (*krb.Client, error) {

	// password
	if o.KerberosPassword != "" {
		return krb.NewWithPassword(o.KerberosUsername, o.KerberosRealm, o.KerberosPassword, &krbconfig.Config{}), nil
	}

	// keytab
	if o.KerberosKeytabPath != "" {
		keytab, err := krbkeytab.Load(o.KerberosKeytabPath)
		if err != nil {
			return nil, err
		}
		return krb.NewWithKeytab(o.KerberosUsername, o.KerberosRealm, keytab, &krbconfig.Config{}), nil
	}

	return nil, nil
}
