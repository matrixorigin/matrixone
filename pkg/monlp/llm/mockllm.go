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

package llm

import (
	"context"

	"github.com/cespare/xxhash"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type MockClient struct {
	model   string
	options string
}

func NewMockClient(model string, options string) (*MockClient, error) {
	switch model {
	case MockEchoModel:
		return &MockClient{model: model, options: options}, nil
	default:
		return nil, moerr.NewInvalidInputf(context.TODO(), "invalid model: %s", model)
	}
}

func (c *MockClient) ChatMsg(ctx context.Context, messages []Message) (string, error) {
	switch c.model {
	case MockEchoModel:
		if len(messages) == 0 {
			return "", moerr.NewInvalidInputf(ctx, "no messages")
		}
		return messages[len(messages)-1].Content, nil
	}
	return "", moerr.NewInvalidInputf(ctx, "invalid model: %s", c.model)
}

func (c *MockClient) Chat(ctx context.Context, prompt string) (string, error) {
	messages, err := stringToMessage(prompt)
	if err != nil {
		return "", err
	}
	return c.ChatMsg(ctx, messages)
}

func (c *MockClient) CreateEmbedding(ctx context.Context, text string) ([]float32, error) {
	ret := make([]float32, 2)
	ret[0] = float32(len(text))
	ret[1] = float32(xxhash.Sum64([]byte(text))) / 1e10
	return ret, nil
}
