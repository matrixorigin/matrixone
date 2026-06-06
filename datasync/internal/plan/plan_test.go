package plan

import (
	"reflect"
	"testing"

	"github.com/matrixorigin/datasync/internal/config"
)

func TestBuildTasksAppliesMappingAndExcludes(t *testing.T) {
	cfg := &config.Config{
		Sources: []config.Source{{
			Endpoint: config.Endpoint{Name: "tenant_a", Host: "127.0.0.1", Port: 6001, User: "a:admin", Password: "111"},
			Databases: []config.Database{{
				Name: "src_db", Target: "target_db", ExcludeTables: []string{"skip"},
			}},
		}},
	}
	tables := map[DatabaseKey][]string{
		{SourceName: "tenant_a", Database: "src_db"}: {"keep", "skip"},
	}

	tasks := BuildTasks(cfg, tables)
	if len(tasks) != 1 {
		t.Fatalf("len(tasks) = %d, want 1", len(tasks))
	}
	task := tasks[0]
	if task.SourceName != "tenant_a" ||
		task.SourceHost != "127.0.0.1" ||
		task.SourcePort != 6001 ||
		task.SourceUser != "a:admin" ||
		task.SourcePassword != "111" ||
		task.SourceDatabase != "src_db" ||
		task.SourceTable != "keep" ||
		task.TargetDatabase != "target_db" {
		t.Fatalf("task = %+v", task)
	}
}

func TestBuildTasksPreservesSourceDatabaseAndTableOrder(t *testing.T) {
	cfg := &config.Config{
		Sources: []config.Source{
			{
				Endpoint: config.Endpoint{Name: "tenant_a", Host: "src-a", Port: 6001, User: "a:admin", Password: "a-pass"},
				Databases: []config.Database{
					{Name: "a_db1", Target: "target_a1"},
					{Name: "a_db2", Target: "target_a2"},
				},
			},
			{
				Endpoint: config.Endpoint{Name: "tenant_b", Host: "src-b", Port: 6002, User: "b:admin", Password: "b-pass"},
				Databases: []config.Database{
					{Name: "b_db1", Target: "target_b1"},
				},
			},
		},
	}
	tables := map[DatabaseKey][]string{
		{SourceName: "tenant_a", Database: "a_db1"}: {"a1_t1", "a1_t2"},
		{SourceName: "tenant_a", Database: "a_db2"}: {"a2_t1"},
		{SourceName: "tenant_b", Database: "b_db1"}: {"b1_t1"},
	}

	tasks := BuildTasks(cfg, tables)

	var got []string
	for _, task := range tasks {
		got = append(got, task.SourceName+"."+task.SourceDatabase+"."+task.SourceTable+"->"+task.TargetDatabase)
	}
	want := []string{
		"tenant_a.a_db1.a1_t1->target_a1",
		"tenant_a.a_db1.a1_t2->target_a1",
		"tenant_a.a_db2.a2_t1->target_a2",
		"tenant_b.b_db1.b1_t1->target_b1",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("task order = %#v, want %#v", got, want)
	}
}

func TestBuildTasksUsesExactExcludeMatching(t *testing.T) {
	cfg := &config.Config{
		Sources: []config.Source{{
			Endpoint: config.Endpoint{Name: "tenant_a", Host: "127.0.0.1", Port: 6001, User: "a:admin", Password: "111"},
			Databases: []config.Database{{
				Name: "src_db", Target: "target_db", ExcludeTables: []string{"skip"},
			}},
		}},
	}
	tables := map[DatabaseKey][]string{
		{SourceName: "tenant_a", Database: "src_db"}: {"skip_prefix", "skip", "prefix_skip"},
	}

	tasks := BuildTasks(cfg, tables)

	var got []string
	for _, task := range tasks {
		got = append(got, task.SourceTable)
	}
	want := []string{"skip_prefix", "prefix_skip"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SourceTable values = %#v, want %#v", got, want)
	}
}
