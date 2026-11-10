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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMockEchoModel(t *testing.T) {
	client, err := NewMockClient(MockEchoModel, "")
	if err != nil {
		t.Fatal(err)
	}

	msgs := make([]Message, 0, 3)
	msgs = append(msgs,
		Message{
			Role:    "system",
			Content: "You are a helpful assistant.",
		},
		Message{
			Role:    "user",
			Content: "Hello, world!",
		},
	)

	reply, err := client.ChatMsg(context.Background(), msgs)
	if err != nil {
		t.Fatal(err)
	}

	if reply != "Hello, world!" {
		t.Fatal("reply is not correct")
	}

	prompt, err := json.Marshal(msgs)
	if err != nil {
		t.Fatal(err)
	}
	reply, err = client.Chat(context.Background(), string(prompt))
	if err != nil {
		t.Fatal(err)
	}
	if reply != "Hello, world!" {
		t.Fatal("reply is not correct")
	}

	msgs = append(msgs, Message{
		Role:    "assistant",
		Content: reply + " again",
	})

	reply, err = client.ChatMsg(context.Background(), msgs)
	if err != nil {
		t.Fatal(err)
	}

	if reply != "Hello, world! again" {
		t.Fatal("reply is not correct")
	}

	embedding, err := client.CreateEmbedding(context.Background(), "Hello, world!")
	if err != nil {
		t.Fatal(err)
	}

	if embedding[0] != 13 {
		t.Fatal("embedding is not correct")
	}
}

func TestNewLLMClientFailureReturnsNilInterface(t *testing.T) {
	t.Setenv("OLLAMA_HOST", "http://127.0.0.1:11434")
	for _, tc := range []struct{ server, model, options string }{
		{MockServer, "", ""},
		{OllamaServer, "test-model", "{invalid"},
		{"unknown", "echo", ""},
	} {
		client, err := NewLLMClient(tc.server, "", tc.model, tc.options)
		require.Error(t, err)
		require.True(t, client == nil, "failed factory returned a typed-nil %T", client)
	}
	client, err := NewLLMClient(MockServer, "", MockEchoModel, "")
	require.NoError(t, err)
	reply, err := client.Chat(context.Background(), `[{"role":"user","content":"ok"}]`)
	require.NoError(t, err)
	require.Equal(t, "ok", reply)
}
