// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package moconnector

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

type MockSQLExecutor struct {
	execCount    int
	executedSQLs []string
	wg           *sync.WaitGroup
}

func (m *MockSQLExecutor) Exec(ctx context.Context, sql string, opts ie.SessionOverrideOptions) error {
	m.execCount++
	m.executedSQLs = append(m.executedSQLs, sql)
	m.wg.Done() // Decrement the WaitGroup counter after processing a message
	return nil
}

type MysqlResultSet struct {
	//column information
	Columns []string

	//column name --> column index
	Name2Index map[string]uint64

	//data
	Data [][]interface{}
}
type internalExecResult struct {
	affectedRows uint64
	resultSet    *MysqlResultSet
	err          error
}

func (res *internalExecResult) GetUint64(ctx context.Context, u uint64, u2 uint64) (uint64, error) {
	return 0, nil
}

func (res *internalExecResult) Error() error {
	return res.err
}

func (res *internalExecResult) ColumnCount() uint64 {
	return 1
}

func (res *internalExecResult) Column(ctx context.Context, i uint64) (name string, typ uint8, signed bool, err error) {
	return "test", 1, true, nil
}

func (res *internalExecResult) RowCount() uint64 {
	return 1
}

func (res *internalExecResult) Row(ctx context.Context, i uint64) ([]interface{}, error) {
	return nil, nil
}

func (res *internalExecResult) Value(ctx context.Context, ridx uint64, cidx uint64) (interface{}, error) {
	return nil, nil
}

func (res *internalExecResult) GetFloat64(ctx context.Context, ridx uint64, cid uint64) (float64, error) {
	return 0.0, nil
}
func (res *internalExecResult) GetString(ctx context.Context, ridx uint64, cid uint64) (string, error) {
	return "", nil
}

func (m *MockSQLExecutor) Query(ctx context.Context, sql string, pts ie.SessionOverrideOptions) ie.InternalExecResult {
	return &internalExecResult{affectedRows: 1, resultSet: nil, err: moerr.NewInternalError(context.TODO(), "random")}
}

func (m *MockSQLExecutor) ApplySessionOverride(opts ie.SessionOverrideOptions) {}

func TestKafkaMoConnector(t *testing.T) {
	// Setup mock Kafka cluster
	mockCluster, err := kafka.NewMockCluster(1)
	if err != nil {
		t.Fatalf("Failed to create MockCluster: %s", err)
	}
	defer mockCluster.Close()

	broker := mockCluster.BootstrapServers()
	topic := "testTopic"

	var wg sync.WaitGroup
	mockExecutor := &MockSQLExecutor{wg: &wg}

	// Create KafkaMoConnector instance
	options := map[string]string{
		"type":              "kafka",
		"topic":             topic,
		"database":          "testDB",
		"table":             "testTable",
		"value":             "json",
		"bootstrap.servers": broker,
		"sql":               "select * from testDB.testStream",
	}
	rt := runtime.DefaultRuntime()
	connector, err := NewKafkaMoConnector(rt.Logger().RawLogger(), options, mockExecutor, 1)
	if err != nil {
		t.Fatalf("Failed to create KafkaMoConnector: %s", err)
	}

	// Produce mock data
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		t.Fatalf("Failed to create producer: %s", err)
	}
	type MessagePayload struct {
		Name string `json:"name"`
		Age  int32  `json:"age"`
	}
	payload := MessagePayload{
		Name: "test_name",
		Age:  100,
	}
	value, _ := json.Marshal(payload)

	msg_num := 10

	// produce 10 messages
	for i := 0; i < msg_num; i++ {
		wg.Add(1) // Increment the WaitGroup counter for each message
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          value,
		}, nil)
	}
	// Start the connector in a goroutine
	go func() {
		connector.Start(context.Background())
	}()

	// Create a channel to signal when all messages are processed
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	// Wait for all messages to be processed or timeout
	select {
	case <-done:
		// All messages processed
	case <-time.After(30 * time.Second):
		t.Error("Timed out waiting for messages to be processed")
	}

	// Stop the connector
	if err := connector.Cancel(); err != nil {
		t.Errorf("Error in Close: %s", err)
	}

	// Verify that the MockSQLExecutor has executed the correct SQL for 10 times
	if mockExecutor.execCount != msg_num {
		t.Errorf("Expected SQL to be executed 10 times, but got %d", mockExecutor.execCount)
	}
}

var _ taskservice.TaskService = new(testTaskService)

type testTaskService struct {
}

func (testTS *testTaskService) Close() error {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) CreateAsyncTask(ctx context.Context, metadata task.TaskMetadata) error {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) CreateBatch(ctx context.Context, metadata []task.TaskMetadata) error {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) CreateCronTask(ctx context.Context, task task.TaskMetadata, cronExpr string) error {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) Allocate(ctx context.Context, value task.AsyncTask, taskRunner string) error {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) Complete(ctx context.Context, taskRunner string, task task.AsyncTask, result task.ExecuteResult) error {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) Heartbeat(ctx context.Context, task task.AsyncTask) error {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) QueryAsyncTask(ctx context.Context, condition ...taskservice.Condition) ([]task.AsyncTask, error) {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) QueryCronTask(ctx context.Context, condition ...taskservice.Condition) ([]task.CronTask, error) {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) CreateDaemonTask(ctx context.Context, value task.TaskMetadata, details *task.Details) error {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) QueryDaemonTask(ctx context.Context, conds ...taskservice.Condition) ([]task.DaemonTask, error) {
	return nil, moerr.NewInternalErrorNoCtx("return err")
}

func (testTS *testTaskService) UpdateDaemonTask(ctx context.Context, tasks []task.DaemonTask, cond ...taskservice.Condition) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) HeartbeatDaemonTask(ctx context.Context, task task.DaemonTask) error {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) StartScheduleCronTask() {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) StopScheduleCronTask() {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) TruncateCompletedTasks(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) GetStorage() taskservice.TaskStorage {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) AddCDCTask(ctx context.Context, metadata task.TaskMetadata, details *task.Details, f func(context.Context, taskservice.SqlExecutor) (int, error)) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (testTS *testTaskService) UpdateCDCTask(ctx context.Context, status task.TaskStatus, f func(context.Context, task.TaskStatus, map[taskservice.CDCTaskKey]struct{}, taskservice.SqlExecutor) (int, error), condition ...taskservice.Condition) (int, error) {
	//TODO implement me
	panic("implement me")
}

func Test_KafkaSinkConnectorExecutor(t *testing.T) {
	exec := KafkaSinkConnectorExecutor(nil, &testTaskService{}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := exec(ctx, &task.CronTask{})
	assert.Error(t, err)
}
