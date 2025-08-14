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
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/ollama"
)

type OllamaClient struct {
	model       string
	llm         *ollama.LLM
	temperature float64
	opts        map[string]any
}

func NewOllamaClient(addr string, model string, options string) (*OllamaClient, error) {
	var cli OllamaClient
	var err error
	cli.model = model
	// ignore addr, we use default local ollama server
	cli.llm, err = ollama.New(ollama.WithModel(model))
	if err != nil {
		return nil, err
	}

	if options != "" {
		err = json.Unmarshal([]byte(options), &cli.opts)
		if err != nil {
			return nil, err
		}
	}

	temp, ok := cli.opts["temperature"]
	if ok {
		cli.temperature, ok = temp.(float64)
		if !ok || cli.temperature < 0 || cli.temperature > 1 {
			return nil, moerr.NewInvalidInputf(context.TODO(), "invalid temperature: %v", temp)
		}
	} else {
		// default temperature is 0.1, it is relatively low so the model will be more deterministic
		cli.temperature = 0.1
	}

	return &cli, nil
}

func mapRole(role string) llms.ChatMessageType {
	switch role {
	case LLMRoleSystem:
		return llms.ChatMessageTypeSystem
	case LLMRoleUser:
		return llms.ChatMessageTypeHuman
	case LLMRoleAI:
		return llms.ChatMessageTypeAI
	default:
		return llms.ChatMessageTypeGeneric
	}
}

func (o *OllamaClient) ChatMsg(ctx context.Context, messages []Message) (string, error) {
	if o.llm == nil {
		return "", moerr.NewInvalidInputf(ctx, "ollama client not initialized")
	}

	chatMessages := make([]llms.MessageContent, len(messages))
	for i, msg := range messages {
		chatMessages[i] = llms.TextParts(mapRole(msg.Role), msg.Content)
	}

	response, err := o.llm.GenerateContent(ctx, chatMessages, llms.WithTemperature(o.temperature))
	if err != nil {
		return "", err
	}

	if len(response.Choices) == 0 {
		return "", moerr.NewInternalError(ctx, "no response from ollama")
	}
	// TODO: handle multiple choices, currently we only return the first choice.
	return response.Choices[0].Content, nil
}

func (o *OllamaClient) Chat(ctx context.Context, prompt string) (string, error) {
	messages, err := stringToMessage(prompt)
	if err != nil {
		return "", err
	}
	return o.ChatMsg(ctx, messages)
}

func (o *OllamaClient) CreateEmbedding(ctx context.Context, text string) ([]float32, error) {
	if o.llm == nil {
		return nil, moerr.NewInvalidInputf(ctx, "ollama client not initialized")
	}

	ret, err := o.llm.CreateEmbedding(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	if len(ret) == 0 {
		return nil, moerr.NewInternalError(ctx, "no response from ollama")
	}
	return ret[0], nil
}
