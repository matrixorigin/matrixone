// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// Message represents a chat message.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Temperature float64   `json:"temperature"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

// Client is an OpenAI-compatible LLM API client.
type Client struct {
	apiURL string
	apiKey string
	model  string
}

// NewClient creates an LLM client.
func NewClient(apiURL, apiKey, model string) *Client {
	return &Client{apiURL: apiURL, apiKey: apiKey, model: model}
}

// Chat sends messages to the LLM and returns the response text.
func (c *Client) Chat(ctx context.Context, messages []Message) (string, error) {
	reqBody := chatRequest{
		Model:       c.model,
		Messages:    messages,
		Temperature: 0.1,
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return "", moerr.NewInternalErrorNoCtxf("marshal request: %v", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.apiURL, bytes.NewReader(data))
	if err != nil {
		return "", moerr.NewInternalErrorNoCtxf("create request: %v", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return "", moerr.NewInternalErrorNoCtxf("LLM API call failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", moerr.NewInternalErrorNoCtxf("read response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", moerr.NewInternalErrorNoCtxf("LLM API %d: %s", resp.StatusCode, string(body))
	}

	var chatResp chatResponse
	if err := json.Unmarshal(body, &chatResp); err != nil {
		return "", moerr.NewInternalErrorNoCtxf("parse response: %v", err)
	}

	if len(chatResp.Choices) == 0 {
		return "", moerr.NewInternalErrorNoCtx("LLM returned no choices")
	}

	return chatResp.Choices[0].Message.Content, nil
}
