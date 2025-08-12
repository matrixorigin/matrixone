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
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	MockServer   = ""
	OllamaServer = "ollama"

	MockEchoModel = "echo"

	LLMRoleSystem = "system"
	LLMRoleUser   = "user"
	LLMRoleAI     = "ai"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type LLMClient interface {
	ChatMsg(ctx context.Context, messages []Message) (string, error)
	Chat(ctx context.Context, prompt string) (string, error)
}

func NewLLMClient(server string, addr string, model string, options string) (LLMClient, error) {
	switch server {
	case MockServer:
		return NewMockClient(model, options)
	case OllamaServer:
		return NewOllamaClient(addr, model, options)
	default:
		return nil, moerr.NewInvalidInputf(context.TODO(), "invalid server: %s", server)
	}
}

func stringToMessage(prompt string) ([]Message, error) {
	var messages []Message
	err := json.Unmarshal([]byte(prompt), &messages)
	if err != nil {
		return nil, err
	}
	return messages, nil
}
