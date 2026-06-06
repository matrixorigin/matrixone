package plan

import (
	"reflect"
	"testing"

	"github.com/matrixorigin/datasync/internal/config"
)

func TestBuildTasksAppliesIncludeThenExclude(t *testing.T) {
	cfg := &config.Config{
		Databases: []config.DatabaseTask{{
			Source:        config.DatabaseEndpoint{Name: "tenant_a", Host: "src", Port: 6001, User: "a:admin", Password: "111", Database: "src_db"},
			Target:        config.DatabaseEndpoint{Name: "target_a", Host: "dst", Port: 6002, User: "target:admin", Password: "222", Database: "dst_db"},
			IncludeTables: []string{"keep", "skip", "missing"},
			ExcludeTables: []string{"skip"},
		}},
	}
	tables := map[DatabaseKey][]string{
		{SourceName: "tenant_a", Database: "src_db"}: {"keep", "skip", "other"},
	}

	tasks := BuildTasks(cfg, tables)
	if len(tasks) != 1 {
		t.Fatalf("len(tasks) = %d, want 1", len(tasks))
	}
	task := tasks[0]
	if task.SourceName != "tenant_a" ||
		task.SourceHost != "src" ||
		task.SourcePort != 6001 ||
		task.SourceUser != "a:admin" ||
		task.SourcePassword != "111" ||
		task.SourceDatabase != "src_db" ||
		task.SourceTable != "keep" ||
		task.TargetName != "target_a" ||
		task.TargetHost != "dst" ||
		task.TargetPort != 6002 ||
		task.TargetUser != "target:admin" ||
		task.TargetPassword != "222" ||
		task.TargetDatabase != "dst_db" {
		t.Fatalf("task = %+v", task)
	}
}

func TestTaskTargetEndpoint(t *testing.T) {
	task := Task{
		TargetName:     "target_a",
		TargetHost:     "127.0.0.1",
		TargetPort:     6001,
		TargetUser:     "target:admin",
		TargetPassword: "111",
	}

	endpoint := task.TargetEndpoint()

	if endpoint.Name != "target_a" ||
		endpoint.Host != "127.0.0.1" ||
		endpoint.Port != 6001 ||
		endpoint.User != "target:admin" ||
		endpoint.Password != "111" {
		t.Fatalf("TargetEndpoint() = %+v", endpoint)
	}
}

func TestBuildTasksWithoutIncludeExportsDiscoveredMinusExclude(t *testing.T) {
	cfg := &config.Config{
		Databases: []config.DatabaseTask{{
			Source:        config.DatabaseEndpoint{Name: "tenant_a", Host: "src", Port: 6001, User: "a:admin", Password: "111", Database: "src_db"},
			Target:        config.DatabaseEndpoint{Name: "target_a", Host: "dst", Port: 6002, User: "target:admin", Password: "222", Database: "dst_db"},
			ExcludeTables: []string{"skip"},
		}},
	}
	tables := map[DatabaseKey][]string{
		{SourceName: "tenant_a", Database: "src_db"}: {"keep", "skip", "other"},
	}

	tasks := BuildTasks(cfg, tables)

	var got []string
	for _, task := range tasks {
		got = append(got, task.SourceTable)
	}
	want := []string{"keep", "other"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SourceTable values = %#v, want %#v", got, want)
	}
}

func TestBuildTasksPreservesDatabaseAndTableOrder(t *testing.T) {
	cfg := &config.Config{
		Databases: []config.DatabaseTask{
			{
				Source: config.DatabaseEndpoint{Name: "tenant_a", Host: "src-a", Port: 6001, User: "a:admin", Password: "a-pass", Database: "a_db1"},
				Target: config.DatabaseEndpoint{Name: "target_a", Host: "dst-a", Port: 6001, User: "target-a:admin", Password: "target-pass", Database: "target_a1"},
			},
			{
				Source:        config.DatabaseEndpoint{Name: "tenant_b", Host: "src-b", Port: 6002, User: "b:admin", Password: "b-pass", Database: "b_db1"},
				Target:        config.DatabaseEndpoint{Name: "target_b", Host: "dst-b", Port: 6002, User: "target-b:admin", Password: "target-pass", Database: "target_b1"},
				IncludeTables: []string{"b1_t2", "b1_t1"},
			},
		},
	}
	tables := map[DatabaseKey][]string{
		{SourceName: "tenant_a", Database: "a_db1"}: {"a1_t1", "a1_t2"},
		{SourceName: "tenant_b", Database: "b_db1"}: {"b1_t1", "b1_t2"},
	}

	tasks := BuildTasks(cfg, tables)

	var got []string
	for _, task := range tasks {
		got = append(got, task.SourceName+"."+task.SourceDatabase+"."+task.SourceTable+"->"+task.TargetName+"."+task.TargetDatabase)
	}
	want := []string{
		"tenant_a.a_db1.a1_t1->target_a.target_a1",
		"tenant_a.a_db1.a1_t2->target_a.target_a1",
		"tenant_b.b_db1.b1_t2->target_b.target_b1",
		"tenant_b.b_db1.b1_t1->target_b.target_b1",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("task order = %#v, want %#v", got, want)
	}
}
