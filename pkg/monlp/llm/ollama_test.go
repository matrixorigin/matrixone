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
	"strings"
	"testing"

	"github.com/tmc/langchaingo/llms"
)

const (
	testModel = "qwen2.5:14b"
)

func TestOllamaParams(t *testing.T) {
	client, err := NewOllamaClient("", testModel, "[\"temperature\", 0.1]")
	if client != nil || err == nil {
		t.Error("should not create ollama client with invalid opitons, must be json object")
	}

	client, err = NewOllamaClient("", testModel, "{\"temperature\": 2.5}")
	if client != nil || err == nil {
		t.Error("should not create ollama client with invalid temperature")
	}

	client, err = NewOllamaClient("", testModel, "{\"temperature\": 0.5}")
	if client == nil || err != nil || client.temperature != 0.5 {
		t.Error("should create ollama client with valid temperature 0.5")
	}

	client, err = NewOllamaClient("", testModel, "")
	if client == nil || err != nil || client.temperature != 0.1 {
		t.Error("should create ollama client with valid temperature 0.1")
	}

	// Use factory method
	cli, err := NewLLMClient(OllamaServer, "", testModel, "")
	if cli == nil || err != nil {
		t.Error("should create ollama client with valid temperature 0.1")
	}

	cli, err = NewLLMClient("foo", "", testModel, "")
	if cli != nil || err == nil {
		t.Error("should fail to create ollama client with invalid server")
	}
}
func TestOllamaError(t *testing.T) {
	msgs := []Message{
		{
			Role:    LLMRoleSystem,
			Content: "You are a helpful assistant.",
		},
		{
			Role:    LLMRoleUser,
			Content: "2+2=?",
		},
	}

	// bad model, but this will still create a client.
	client, err := NewOllamaClient("", "bad-model", "")
	if client == nil || err != nil {
		t.Error("should create ollama client with valid temperature 0.1")
	}

	_, err = client.ChatMsg(context.Background(), msgs)
	if err == nil {
		t.Error("should fail to chat with bad model")
	}

	prompt, err := json.Marshal(msgs)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Chat(context.Background(), string(prompt))
	if err == nil {
		t.Error("should fail to chat with bad model")
	}
}

func TestOllama(t *testing.T) {
	t.Skip("skip ollama test, need to install ollama server")
	msgs := []Message{
		{
			Role:    LLMRoleSystem,
			Content: "You are a helpful assistant.",
		},
		{
			Role:    LLMRoleUser,
			Content: "2+2=?",
		},
	}
	client, err := NewOllamaClient("", testModel, "")
	if client == nil || err != nil {
		t.Error("should create ollama client with valid temperature 0.1")
	}

	reply, err := client.ChatMsg(context.Background(), msgs)
	if err != nil || !strings.Contains(reply, "4") {
		t.Errorf("wrong reply: %v, %s", err, reply)
	}

	prompt, err := json.Marshal(msgs)
	if err != nil {
		t.Fatal(err)
	}
	reply, err = client.Chat(context.Background(), string(prompt))
	if err != nil || !strings.Contains(reply, "4") {
		t.Errorf("wrong reply: %v, %s", err, reply)
	}
}

func TestMapRole(t *testing.T) {
	tests := []struct {
		role string
		want llms.ChatMessageType
	}{
		{LLMRoleSystem, llms.ChatMessageTypeSystem},
		{LLMRoleUser, llms.ChatMessageTypeHuman},
		{LLMRoleAI, llms.ChatMessageTypeAI},
		{"foo", llms.ChatMessageTypeGeneric},
	}
	for _, test := range tests {
		got := mapRole(test.role)
		if got != test.want {
			t.Errorf("mapRole(%s) = %v, want %v", test.role, got, test.want)
		}
	}
}
