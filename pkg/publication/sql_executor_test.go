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

package publication

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetGetParameterUnitWrapper(t *testing.T) {
	// Reset to nil after test
	defer func() {
		SetGetParameterUnitWrapper(nil)
	}()

	// Initially nil
	getParameterUnitWrapperMu.RLock()
	assert.Nil(t, getParameterUnitWrapper)
	getParameterUnitWrapperMu.RUnlock()

	// Set wrapper
	called := false
	testWrapper := func(cnUUID string) *config.ParameterUnit {
		called = true
		return &config.ParameterUnit{}
	}
	SetGetParameterUnitWrapper(testWrapper)

	// Verify wrapper is set
	getParameterUnitWrapperMu.RLock()
	wrapper := getParameterUnitWrapper
	getParameterUnitWrapperMu.RUnlock()
	assert.NotNil(t, wrapper)

	// Call wrapper
	_ = wrapper("test")
	assert.True(t, called)
}

func TestResult_Close(t *testing.T) {
	t.Run("nil result", func(t *testing.T) {
		r := &Result{}
		err := r.Close()
		assert.NoError(t, err)
	})

	t.Run("with internal result", func(t *testing.T) {
		r := &Result{
			internalResult: &InternalResult{},
		}
		err := r.Close()
		assert.NoError(t, err)
	})
}

func TestResult_Next(t *testing.T) {
	t.Run("nil result", func(t *testing.T) {
		r := &Result{}
		assert.False(t, r.Next())
	})

	t.Run("with empty internal result", func(t *testing.T) {
		r := &Result{
			internalResult: &InternalResult{},
		}
		assert.False(t, r.Next())
	})
}

func TestResult_Scan(t *testing.T) {
	t.Run("nil result", func(t *testing.T) {
		r := &Result{}
		var s string
		err := r.Scan(&s)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "result is nil")
	})
}

func TestResult_Err(t *testing.T) {
	t.Run("nil result", func(t *testing.T) {
		r := &Result{}
		assert.Nil(t, r.Err())
	})

	t.Run("with internal result", func(t *testing.T) {
		r := &Result{
			internalResult: &InternalResult{},
		}
		assert.Nil(t, r.Err())
	})
}

func TestParseUpstreamConn(t *testing.T) {
	tests := []struct {
		name        string
		connStr     string
		wantAccount string
		wantUser    string
		wantHost    string
		wantPort    int
		wantErr     bool
		errContains string
	}{
		{
			name:        "valid connection with account",
			connStr:     "mysql://acc#user:password@127.0.0.1:6001",
			wantAccount: "acc",
			wantUser:    "user",
			wantHost:    "127.0.0.1",
			wantPort:    6001,
			wantErr:     false,
		},
		{
			name:        "valid connection without account",
			connStr:     "mysql://user:password@127.0.0.1:6001",
			wantAccount: "",
			wantUser:    "user",
			wantHost:    "127.0.0.1",
			wantPort:    6001,
			wantErr:     false,
		},
		{
			name:        "valid connection with complex password",
			connStr:     "mysql://acc#user:pass:word@localhost:3306",
			wantAccount: "acc",
			wantUser:    "user",
			wantHost:    "localhost",
			wantPort:    3306,
			wantErr:     false,
		},
		{
			name:        "valid connection with query parameters",
			connStr:     "mysql://acc#user:password@127.0.0.1:6001/dbname?param=value",
			wantAccount: "acc",
			wantUser:    "user",
			wantHost:    "127.0.0.1",
			wantPort:    6001,
			wantErr:     false,
		},
		{
			name:        "empty connection string",
			connStr:     "",
			wantErr:     true,
			errContains: "empty",
		},
		{
			name:        "missing mysql prefix",
			connStr:     "postgresql://user:pass@host:5432",
			wantErr:     true,
			errContains: "expected mysql://",
		},
		{
			name:        "missing @ separator",
			connStr:     "mysql://user:password",
			wantErr:     true,
			errContains: "expected mysql://",
		},
		{
			name:        "empty user with account",
			connStr:     "mysql://acc#:password@127.0.0.1:6001",
			wantErr:     true,
			errContains: "user cannot be empty",
		},
		{
			name:        "empty user without account",
			connStr:     "mysql://:password@127.0.0.1:6001",
			wantErr:     true,
			errContains: "user cannot be empty",
		},
		{
			name:        "empty password",
			connStr:     "mysql://user:@127.0.0.1:6001",
			wantErr:     true,
			errContains: "password cannot be empty",
		},
		{
			name:        "empty host",
			connStr:     "mysql://user:password@:6001",
			wantErr:     true,
			errContains: "host cannot be empty",
		},
		{
			name:        "invalid port",
			connStr:     "mysql://user:password@127.0.0.1:abc",
			wantErr:     true,
			errContains: "invalid port",
		},
		{
			name:        "missing port",
			connStr:     "mysql://user:password@127.0.0.1",
			wantErr:     true,
			errContains: "host:port",
		},
		{
			name:        "account with empty user after hash",
			connStr:     "mysql://acc#:password@127.0.0.1:6001",
			wantErr:     true,
			errContains: "user cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := ParseUpstreamConn(tt.connStr)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantAccount, config.Account)
			assert.Equal(t, tt.wantUser, config.User)
			assert.Equal(t, tt.wantHost, config.Host)
			assert.Equal(t, tt.wantPort, config.Port)
			assert.NotEmpty(t, config.Timeout)
		})
	}
}

func TestParseUpstreamConnWithDecrypt(t *testing.T) {
	t.Run("without executor", func(t *testing.T) {
		config, err := ParseUpstreamConnWithDecrypt(
			context.Background(),
			"mysql://acc#user:password@127.0.0.1:6001",
			nil,
			"",
		)
		require.NoError(t, err)
		assert.Equal(t, "acc", config.Account)
		assert.Equal(t, "user", config.User)
		assert.Equal(t, "password", config.Password) // Short password, not encrypted
		assert.Equal(t, "127.0.0.1", config.Host)
		assert.Equal(t, 6001, config.Port)
	})
}

func TestTryDecryptPassword(t *testing.T) {
	t.Run("short password not encrypted", func(t *testing.T) {
		result := tryDecryptPassword(context.Background(), "short", nil, "")
		assert.Equal(t, "short", result)
	})

	t.Run("non-hex string not encrypted", func(t *testing.T) {
		result := tryDecryptPassword(context.Background(), "this-is-not-a-hex-string-at-all!", nil, "")
		assert.Equal(t, "this-is-not-a-hex-string-at-all!", result)
	})

	t.Run("no executor provided", func(t *testing.T) {
		// Long hex string but no executor
		hexStr := "0123456789abcdef0123456789abcdef0123456789abcdef"
		result := tryDecryptPassword(context.Background(), hexStr, nil, "")
		assert.Equal(t, hexStr, result)
	})
}

func TestNewUpstreamExecutor_Validation(t *testing.T) {
	t.Run("empty user", func(t *testing.T) {
		_, err := NewUpstreamExecutor("acc", "", "pass", "127.0.0.1", 6001, 3, time.Minute, "10s", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "user cannot be empty")
	})

	t.Run("account provided but empty user", func(t *testing.T) {
		_, err := NewUpstreamExecutor("acc", "", "pass", "127.0.0.1", 6001, 3, time.Minute, "10s", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "user cannot be empty")
	})
}

func TestUpstreamExecutor_EndTxn(t *testing.T) {
	t.Run("nil transaction", func(t *testing.T) {
		e := &UpstreamExecutor{}
		err := e.EndTxn(context.Background(), true)
		assert.NoError(t, err) // Idempotent
	})
}

func TestUpstreamExecutor_Close(t *testing.T) {
	t.Run("nil connection", func(t *testing.T) {
		e := &UpstreamExecutor{}
		err := e.Close()
		assert.NoError(t, err)
	})
}

func TestUpstreamExecutor_EnsureConnection(t *testing.T) {
	t.Run("already connected", func(t *testing.T) {
		// Mock a non-nil connection scenario
		e := &UpstreamExecutor{
			ip:   "invalid-host",
			port: 99999,
		}
		// conn is nil, will try to connect and fail
		err := e.ensureConnection(context.Background())
		assert.Error(t, err)
	})
}

func TestUpstreamExecutor_ExecSQL_UseTxn(t *testing.T) {
	t.Run("useTxn not supported", func(t *testing.T) {
		e := &UpstreamExecutor{}
		_, _, err := e.ExecSQL(context.Background(), nil, "SELECT 1", true, false, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not support transactions")
	})
}

func TestUpstreamExecutor_CalculateMaxAttempts(t *testing.T) {
	tests := []struct {
		name       string
		retryTimes int
		expected   int
	}{
		{
			name:       "zero retries",
			retryTimes: 0,
			expected:   1,
		},
		{
			name:       "positive retries",
			retryTimes: 3,
			expected:   4,
		},
		{
			name:       "infinite retries",
			retryTimes: -1,
			expected:   2147483647, // math.MaxInt32
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &UpstreamExecutor{retryTimes: tt.retryTimes}
			assert.Equal(t, tt.expected, e.calculateMaxAttempts())
		})
	}
}

func TestUpstreamExecutor_InitRetryPolicy(t *testing.T) {
	classifier := &mockClassifier{retryable: true}
	e := &UpstreamExecutor{retryTimes: 5}
	e.initRetryPolicy(classifier)

	assert.NotNil(t, e.retryPolicy)
	assert.Equal(t, 6, e.retryPolicy.MaxAttempts) // retryTimes + 1
	assert.Equal(t, classifier, e.retryClassifier)
}

func TestUpstreamExecutor_LogFailedSQL(t *testing.T) {
	e := &UpstreamExecutor{}

	// Should not panic
	t.Run("short SQL", func(t *testing.T) {
		e.logFailedSQL(assert.AnError, "SELECT 1")
	})

	t.Run("long SQL", func(t *testing.T) {
		longSQL := make([]byte, 500)
		for i := range longSQL {
			longSQL[i] = 'a'
		}
		e.logFailedSQL(assert.AnError, string(longSQL))
	})
}

func TestActiveRoutine(t *testing.T) {
	t.Run("create and channels", func(t *testing.T) {
		ar := NewActiveRoutine()
		require.NotNil(t, ar)
		assert.NotNil(t, ar.Pause)
		assert.NotNil(t, ar.Cancel)
	})

	t.Run("close pause", func(t *testing.T) {
		ar := NewActiveRoutine()

		// Should not panic
		ar.ClosePause()

		// Channel should be closed
		select {
		case <-ar.Pause:
			// Expected
		default:
			t.Error("Pause channel should be closed")
		}
	})

	t.Run("close cancel", func(t *testing.T) {
		ar := NewActiveRoutine()

		// Should not panic
		ar.CloseCancel()

		// Channel should be closed
		select {
		case <-ar.Cancel:
			// Expected
		default:
			t.Error("Cancel channel should be closed")
		}
	})
}

func TestUpstreamConnConfig(t *testing.T) {
	config := &UpstreamConnConfig{
		Account:  "test_account",
		User:     "test_user",
		Password: "test_password",
		Host:     "127.0.0.1",
		Port:     6001,
		Timeout:  "10s",
	}

	assert.Equal(t, "test_account", config.Account)
	assert.Equal(t, "test_user", config.User)
	assert.Equal(t, "test_password", config.Password)
	assert.Equal(t, "127.0.0.1", config.Host)
	assert.Equal(t, 6001, config.Port)
	assert.Equal(t, "10s", config.Timeout)
}

func TestUpstreamExecutor_ExecWithRetry(t *testing.T) {
	t.Run("context cancelled", func(t *testing.T) {
		e := &UpstreamExecutor{
			retryTimes: 3,
		}
		e.initRetryPolicy(&mockClassifier{retryable: true})

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, _, err := e.execWithRetry(ctx, nil, 0, func(ctx context.Context) (*Result, error) {
			return nil, assert.AnError
		})
		assert.Error(t, err)
	})

	t.Run("active routine paused", func(t *testing.T) {
		e := &UpstreamExecutor{
			retryTimes: 3,
		}
		e.initRetryPolicy(&mockClassifier{retryable: true})

		ar := NewActiveRoutine()
		ar.ClosePause() // Close pause channel

		_, _, err := e.execWithRetry(context.Background(), ar, 0, func(ctx context.Context) (*Result, error) {
			return nil, assert.AnError
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "paused")
	})

	t.Run("active routine cancelled", func(t *testing.T) {
		e := &UpstreamExecutor{
			retryTimes: 3,
		}
		e.initRetryPolicy(&mockClassifier{retryable: true})

		ar := NewActiveRoutine()
		ar.CloseCancel() // Close cancel channel

		_, _, err := e.execWithRetry(context.Background(), ar, 0, func(ctx context.Context) (*Result, error) {
			return nil, assert.AnError
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cancelled")
	})

	t.Run("success on first attempt", func(t *testing.T) {
		e := &UpstreamExecutor{
			retryTimes: 3,
		}
		e.initRetryPolicy(&mockClassifier{retryable: true})

		expectedResult := &Result{}
		result, _, err := e.execWithRetry(context.Background(), nil, 0, func(ctx context.Context) (*Result, error) {
			return expectedResult, nil
		})
		assert.NoError(t, err)
		assert.Equal(t, expectedResult, result)
	})

	t.Run("non-retryable error", func(t *testing.T) {
		e := &UpstreamExecutor{
			retryTimes: 3,
		}
		e.initRetryPolicy(&mockClassifier{retryable: false})

		_, _, err := e.execWithRetry(context.Background(), nil, 0, func(ctx context.Context) (*Result, error) {
			return nil, assert.AnError
		})
		assert.Error(t, err)
	})
}

func TestOpenDbConn_Validation(t *testing.T) {
	t.Run("account provided but user empty", func(t *testing.T) {
		_, err := openDbConn("account", "", "pass", "127.0.0.1", 6001, "10s")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "user is empty")
	})

	t.Run("both account and user empty", func(t *testing.T) {
		_, err := openDbConn("", "", "pass", "127.0.0.1", 6001, "10s")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "user cannot be empty")
	})
}

// mockSQLExecutor is a mock implementation of SQLExecutor for testing
type mockSQLExecutor struct {
	execSQLFunc func(ctx context.Context, ar *ActiveRoutine, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error)
}

func (m *mockSQLExecutor) Close() error {
	return nil
}

func (m *mockSQLExecutor) Connect() error {
	return nil
}

func (m *mockSQLExecutor) EndTxn(ctx context.Context, commit bool) error {
	return nil
}

func (m *mockSQLExecutor) ExecSQL(ctx context.Context, ar *ActiveRoutine, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
	if m.execSQLFunc != nil {
		return m.execSQLFunc(ctx, ar, query, useTxn, needRetry, timeout)
	}
	return nil, func() {}, nil
}

// testMockResult is a mock implementation for testing that simulates Result behavior
type testMockResult struct {
	data       [][]interface{}
	currentRow int
	closed     bool
}

func (r *testMockResult) Close() error {
	r.closed = true
	return nil
}

func (r *testMockResult) Next() bool {
	r.currentRow++
	return r.currentRow < len(r.data)
}

func (r *testMockResult) Scan(dest ...interface{}) error {
	if r.currentRow < 0 || r.currentRow >= len(r.data) {
		return moerr.NewInternalErrorNoCtx("no more rows")
	}
	row := r.data[r.currentRow]
	if len(row) != len(dest) {
		return moerr.NewInternalErrorNoCtx("column count mismatch")
	}
	for i, v := range row {
		switch d := dest[i].(type) {
		case *string:
			if s, ok := v.(string); ok {
				*d = s
			} else {
				return moerr.NewInternalErrorNoCtx("type mismatch: expected string")
			}
		case *int:
			if n, ok := v.(int); ok {
				*d = n
			} else {
				return moerr.NewInternalErrorNoCtx("type mismatch: expected int")
			}
		default:
			return moerr.NewInternalErrorNoCtx("unsupported type")
		}
	}
	return nil
}

func (r *testMockResult) Err() error {
	return nil
}

// mockResultForTest creates a mock Result with given data for testing
func mockResultForTest(data [][]interface{}) *Result {
	mock := &testMockResult{
		data:       data,
		currentRow: -1,
	}
	return &Result{
		mockResult: mock,
	}
}

func TestInitAesKeyForPublication(t *testing.T) {
	// Save original AesKey and restore after test
	originalAesKey := cdc.AesKey
	defer func() {
		cdc.AesKey = originalAesKey
	}()

	// Save original wrapper and restore after test
	defer func() {
		SetGetParameterUnitWrapper(nil)
	}()

	t.Run("already initialized", func(t *testing.T) {
		cdc.AesKey = "test-key-already-set-12345678901"

		err := initAesKeyForPublication(context.Background(), nil, "test-cn-uuid")
		assert.NoError(t, err)
	})

	t.Run("executor returns error", func(t *testing.T) {
		cdc.AesKey = ""

		mockExec := &mockSQLExecutor{
			execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
				return nil, func() {}, moerr.NewInternalErrorNoCtx("exec error")
			},
		}

		err := initAesKeyForPublication(context.Background(), mockExec, "test-cn-uuid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exec error")
	})

	t.Run("no data key found", func(t *testing.T) {
		cdc.AesKey = ""

		mockExec := &mockSQLExecutor{
			execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
				// Return empty result (no rows)
				return mockResultForTest([][]interface{}{}), func() {}, nil
			},
		}

		err := initAesKeyForPublication(context.Background(), mockExec, "test-cn-uuid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no data key found")
	})

	t.Run("scan error", func(t *testing.T) {
		cdc.AesKey = ""

		mockExec := &mockSQLExecutor{
			execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
				// Return result with wrong type that will cause scan error
				return mockResultForTest([][]interface{}{
					{123}, // int instead of string
				}), func() {}, nil
			},
		}

		err := initAesKeyForPublication(context.Background(), mockExec, "test-cn-uuid")
		assert.Error(t, err)
	})

	t.Run("parameter unit not available - no wrapper", func(t *testing.T) {
		cdc.AesKey = ""
		SetGetParameterUnitWrapper(nil)

		mockExec := &mockSQLExecutor{
			execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
				return mockResultForTest([][]interface{}{
					{"encrypted-key-data"},
				}), func() {}, nil
			},
		}

		err := initAesKeyForPublication(context.Background(), mockExec, "test-cn-uuid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ParameterUnit not available")
	})

	t.Run("parameter unit not available - wrapper returns nil", func(t *testing.T) {
		cdc.AesKey = ""
		SetGetParameterUnitWrapper(func(cnUUID string) *config.ParameterUnit {
			return nil
		})

		mockExec := &mockSQLExecutor{
			execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
				return mockResultForTest([][]interface{}{
					{"encrypted-key-data"},
				}), func() {}, nil
			},
		}

		err := initAesKeyForPublication(context.Background(), mockExec, "test-cn-uuid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ParameterUnit not available")
	})

	t.Run("parameter unit SV is nil", func(t *testing.T) {
		cdc.AesKey = ""
		SetGetParameterUnitWrapper(func(cnUUID string) *config.ParameterUnit {
			return &config.ParameterUnit{
				SV: nil,
			}
		})

		mockExec := &mockSQLExecutor{
			execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
				return mockResultForTest([][]interface{}{
					{"encrypted-key-data"},
				}), func() {}, nil
			},
		}

		err := initAesKeyForPublication(context.Background(), mockExec, "test-cn-uuid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ParameterUnit not available")
	})

	t.Run("fallback to context ParameterUnit", func(t *testing.T) {
		cdc.AesKey = ""
		SetGetParameterUnitWrapper(nil) // No wrapper

		testKEK := "test-kek-key-32-bytes-long-1234"
		testDataKey := "test-data-key-32-bytes-long-123"
		fakeEncryptedKey := "0123456789abcdef0123456789abcdef0123456789abcdef" // fake encrypted data

		// Mock AesCFBDecodeWithKey to return our test data key
		stub := gostub.Stub(&cdc.AesCFBDecodeWithKey, func(ctx context.Context, data string, aesKey []byte) (string, error) {
			return testDataKey, nil
		})
		defer stub.Reset()

		mockExec := &mockSQLExecutor{
			execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
				return mockResultForTest([][]interface{}{
					{fakeEncryptedKey},
				}), func() {}, nil
			},
		}

		// Create context with ParameterUnit
		pu := &config.ParameterUnit{
			SV: &config.FrontendParameters{
				KeyEncryptionKey: testKEK,
			},
		}
		ctx := context.WithValue(context.Background(), config.ParameterUnitKey, pu)

		err := initAesKeyForPublication(ctx, mockExec, "test-cn-uuid")
		assert.NoError(t, err)
		assert.NotEmpty(t, cdc.AesKey)
		assert.Equal(t, testDataKey, cdc.AesKey)
	})

	t.Run("success with wrapper", func(t *testing.T) {
		cdc.AesKey = ""

		testKEK := "test-kek-key-32-bytes-long-1234"
		testDataKey := "test-data-key-32-bytes-long-456"
		fakeEncryptedKey := "0123456789abcdef0123456789abcdef0123456789abcdef" // fake encrypted data

		// Mock AesCFBDecodeWithKey to return our test data key
		stub := gostub.Stub(&cdc.AesCFBDecodeWithKey, func(ctx context.Context, data string, aesKey []byte) (string, error) {
			return testDataKey, nil
		})
		defer stub.Reset()

		SetGetParameterUnitWrapper(func(cnUUID string) *config.ParameterUnit {
			return &config.ParameterUnit{
				SV: &config.FrontendParameters{
					KeyEncryptionKey: testKEK,
				},
			}
		})

		mockExec := &mockSQLExecutor{
			execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
				return mockResultForTest([][]interface{}{
					{fakeEncryptedKey},
				}), func() {}, nil
			},
		}

		err := initAesKeyForPublication(context.Background(), mockExec, "test-cn-uuid")
		assert.NoError(t, err)
		assert.NotEmpty(t, cdc.AesKey)
		assert.Equal(t, testDataKey, cdc.AesKey)
	})

	t.Run("decrypt error - AesCFBDecodeWithKey returns error", func(t *testing.T) {
		cdc.AesKey = ""

		testKEK := "test-kek-key-32-bytes-long-1234"
		fakeEncryptedKey := "0123456789abcdef0123456789abcdef0123456789abcdef"

		// Mock AesCFBDecodeWithKey to return an error
		stub := gostub.Stub(&cdc.AesCFBDecodeWithKey, func(ctx context.Context, data string, aesKey []byte) (string, error) {
			return "", moerr.NewInternalError(ctx, "decryption failed")
		})
		defer stub.Reset()

		SetGetParameterUnitWrapper(func(cnUUID string) *config.ParameterUnit {
			return &config.ParameterUnit{
				SV: &config.FrontendParameters{
					KeyEncryptionKey: testKEK,
				},
			}
		})

		mockExec := &mockSQLExecutor{
			execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
				return mockResultForTest([][]interface{}{
					{fakeEncryptedKey},
				}), func() {}, nil
			},
		}

		err := initAesKeyForPublication(context.Background(), mockExec, "test-cn-uuid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "decryption failed")
	})
}
