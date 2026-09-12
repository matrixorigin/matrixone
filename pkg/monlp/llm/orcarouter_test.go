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
	"os"
	"testing"
)

func TestOrcaRouterParams(t *testing.T) {
	t.Setenv(orcaRouterAPIKeyEnv, "test-key")

	client, err := NewOrcaRouterClient("", "orcarouter/auto", "[\"temperature\", 0.1]")
	if client != nil || err == nil {
		t.Error("should not create orcarouter client with invalid options, must be json object")
	}

	client, err = NewOrcaRouterClient("", "orcarouter/auto", "{\"temperature\": 2.5}")
	if client != nil || err == nil {
		t.Error("should not create orcarouter client with invalid temperature")
	}

	client, err = NewOrcaRouterClient("", "orcarouter/auto", "{\"temperature\": 0.5}")
	if client == nil || err != nil || client.temperature != 0.5 {
		t.Error("should create orcarouter client with valid temperature 0.5")
	}

	client, err = NewOrcaRouterClient("", "orcarouter/auto", "")
	if client == nil || err != nil || client.temperature != 0.1 {
		t.Error("should create orcarouter client with valid temperature 0.1")
	}

	// default model is used when model is empty
	client, err = NewOrcaRouterClient("", "", "")
	if client == nil || err != nil || client.model != OrcaRouterDefaultModel {
		t.Error("should create orcarouter client with default model")
	}

	// Use factory method
	cli, err := NewLLMClient(OrcaRouterSvr, "", "orcarouter/auto", "")
	if cli == nil || err != nil {
		t.Error("should create orcarouter client via factory with valid temperature 0.1")
	}

	cli, err = NewLLMClient("foo", "", "orcarouter/auto", "")
	if cli != nil || err == nil {
		t.Error("should fail to create orcarouter client with invalid server")
	}
}

func TestOrcaRouterMissingKey(t *testing.T) {
	if v, ok := os.LookupEnv(orcaRouterAPIKeyEnv); ok {
		t.Setenv(orcaRouterAPIKeyEnv, v)
	} else {
		t.Setenv(orcaRouterAPIKeyEnv, "")
	}

	client, err := NewOrcaRouterClient("", "orcarouter/auto", "")
	if client != nil || err == nil {
		t.Error("should fail to create orcarouter client without ORCAROUTER_API_KEY")
	}
}

func TestOrcaRouterEmbeddingNotSupported(t *testing.T) {
	t.Setenv(orcaRouterAPIKeyEnv, "test-key")

	client, err := NewOrcaRouterClient("", "orcarouter/auto", "")
	if client == nil || err != nil {
		t.Fatal("should create orcarouter client with valid key")
	}

	_, err = client.CreateEmbedding(context.Background(), "Hello, world!")
	if err == nil {
		t.Error("should fail to create embedding with orcarouter client")
	}
}
