package plan

import "github.com/matrixorigin/datasync/internal/config"

type DatabaseKey struct {
	SourceName string
	Database   string
}

type Task struct {
	SourceName     string
	SourceHost     string
	SourcePort     int
	SourceUser     string
	SourcePassword string
	SourceDatabase string
	SourceTable    string
	TargetDatabase string
}

func BuildTasks(cfg *config.Config, tables map[DatabaseKey][]string) []Task {
	var tasks []Task
	for _, src := range cfg.Sources {
		for _, database := range src.Databases {
			excluded := make(map[string]struct{}, len(database.ExcludeTables))
			for _, table := range database.ExcludeTables {
				excluded[table] = struct{}{}
			}

			key := DatabaseKey{SourceName: src.Name, Database: database.Name}
			for _, table := range tables[key] {
				if _, ok := excluded[table]; ok {
					continue
				}
				tasks = append(tasks, Task{
					SourceName:     src.Name,
					SourceHost:     src.Host,
					SourcePort:     src.Port,
					SourceUser:     src.User,
					SourcePassword: src.Password,
					SourceDatabase: database.Name,
					SourceTable:    table,
					TargetDatabase: database.Target,
				})
			}
		}
	}
	return tasks
}
