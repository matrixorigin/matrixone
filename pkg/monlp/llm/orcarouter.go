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
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/openai"
)

const (
	OrcaRouterServer = "orcarouter"

	OrcaRouterDefaultBaseURL = "https://api.orcarouter.ai/v1"
	OrcaRouterDefaultModel   = "orcarouter/auto"

	orcaRouterAPIKeyEnv = "ORCAROUTER_API_KEY"
)

type OrcaRouterClient struct {
	model       string
	llm         *openai.LLM
	temperature float64
	opts        map[string]any
}

// NewOrcaRouterClient creates a client that routes chat requests through the
// OrcaRouter gateway (an OpenAI-compatible model router). The api key is read
// from the ORCAROUTER_API_KEY environment variable, and addr is ignored since
// the gateway base URL is fixed to https://api.orcarouter.ai/v1.
func NewOrcaRouterClient(addr string, model string, options string) (*OrcaRouterClient, error) {
	var cli OrcaRouterClient
	var err error
	cli.model = model
	if cli.model == "" {
		cli.model = OrcaRouterDefaultModel
	}

	token := os.Getenv(orcaRouterAPIKeyEnv)
	if token == "" {
		return nil, moerr.NewInvalidInputf(context.TODO(), "missing ORCAROUTER_API_KEY environment variable")
	}

	cli.llm, err = openai.New(
		openai.WithToken(token),
		openai.WithBaseURL(OrcaRouterDefaultBaseURL),
		openai.WithModel(cli.model),
	)
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

func (o *OrcaRouterClient) ChatMsg(ctx context.Context, messages []Message) (string, error) {
	if o.llm == nil {
		return "", moerr.NewInvalidInputf(ctx, "orcarouter client not initialized")
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
		return "", moerr.NewInternalError(ctx, "no response from orcarouter")
	}
	// TODO: handle multiple choices, currently we only return the first choice.
	return response.Choices[0].Content, nil
}

func (o *OrcaRouterClient) Chat(ctx context.Context, prompt string) (string, error) {
	messages, err := stringToMessage(prompt)
	if err != nil {
		return "", err
	}
	return o.ChatMsg(ctx, messages)
}

// CreateEmbedding is not supported by OrcaRouter, which is a chat-only
// gateway that routes LLM chat requests.
func (o *OrcaRouterClient) CreateEmbedding(ctx context.Context, text string) ([]float32, error) {
	return nil, moerr.NewNotSupportedf(ctx, "orcarouter does not support embeddings")
}
