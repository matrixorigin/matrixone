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

package pythonservice

import (
	"context"
	"io"
	"math"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/udf"
)

type Client struct {
	cfg   ClientConfig
	sc    udf.ServiceClient
	mutex sync.Mutex
}

func NewClient(cfg ClientConfig) (*Client, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}
	return &Client{cfg: cfg}, nil
}

func (c *Client) init() error {
	if c.sc == nil {
		err := func() error {
			c.mutex.Lock()
			defer c.mutex.Unlock()
			if c.sc == nil {
				conn, err := grpc.Dial(c.cfg.ServerAddress,
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithDefaultCallOptions(
						grpc.MaxCallSendMsgSize(math.MaxInt32),
						grpc.MaxCallRecvMsgSize(math.MaxInt32),
					),
				)
				if err != nil {
					return err
				}
				c.sc = udf.NewServiceClient(conn)
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) Run(ctx context.Context, request *udf.Request, pkgReader udf.PkgReader) (*udf.Response, error) {
	if request.Udf.Language != udf.LanguagePython {
		return nil, moerr.NewInvalidArg(ctx, "udf language", request.Udf.Language)
	}

	if request.Type != udf.RequestType_DataRequest {
		return nil, moerr.NewInvalidInput(ctx, "type of the first udf request must be 'DataRequest'")
	}

	err := c.init()
	if err != nil {
		return nil, err
	}

	stream, err := c.sc.Run(ctx)
	defer stream.CloseSend()
	if err != nil {
		return nil, err
	}
	err = stream.Send(request)
	if err != nil {
		return nil, err
	}
	response, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	switch response.Type {
	case udf.ResponseType_DataResponse:
		return response, nil
	case udf.ResponseType_PkgRequest:
		var reader io.ReadCloser
		reader, err = pkgReader.Get(ctx, request.Udf.Body)
		if reader != nil {
			defer reader.Close()
		}
		if err != nil {
			return nil, err
		}

		pkgRequest := &udf.Request{
			Udf:     request.Udf,
			Type:    udf.RequestType_PkgResponse,
			Context: request.Context,
		}
		for {
			pkg := &udf.Package{}
			buffer := make([]byte, 5*1024*1024)
			var offset int
			for {
				var n int
				n, err = reader.Read(buffer[offset:])
				if err != nil {
					if err == io.EOF {
						pkg.Last = true
					} else {
						return nil, err
					}
				}
				offset += n

				if pkg.Last || len(buffer) == offset {
					pkg.Data = buffer[:offset]
					break
				}
			}

			pkgRequest.Udf.ImportPkg = pkg
			err = stream.Send(pkgRequest)
			if err != nil {
				return nil, err
			}

			response, err = stream.Recv()
			if err != nil {
				return nil, err
			}

			if pkg.Last {
				return response, nil
			}
		}
	default:
		return nil, moerr.NewInternalError(ctx, "error udf response type")
	}
}

func (c *Client) Language() string {
	return udf.LanguagePython
}
