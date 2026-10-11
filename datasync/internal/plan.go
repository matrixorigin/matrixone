package datasync

import "fmt"

type DatabaseKey struct {
	SourceName string
	SourceHost string
	SourcePort int
	SourceUser string
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
	SourceRows     int64
	TargetName     string
	TargetHost     string
	TargetPort     int
	TargetUser     string
	TargetPassword string
	TargetDatabase string
}

type targetTableKey struct {
	TargetName     string
	TargetHost     string
	TargetPort     int
	TargetUser     string
	TargetDatabase string
	TargetTable    string
}

func (t Task) TargetEndpoint() Endpoint {
	return Endpoint{
		Name:     t.TargetName,
		Host:     t.TargetHost,
		Port:     t.TargetPort,
		User:     t.TargetUser,
		Password: t.TargetPassword,
	}
}

func BuildTasks(cfg *Config, tables map[DatabaseKey][]string) ([]Task, error) {
	var tasks []Task
	plannedTargets := make(map[targetTableKey]Task)
	for _, database := range cfg.Databases {
		excluded := make(map[string]struct{}, len(database.ExcludeTables))
		for _, table := range database.ExcludeTables {
			excluded[table] = struct{}{}
		}

		discovered := tables[databaseKey(database.Source)]
		discoveredSet := make(map[string]struct{}, len(discovered))
		for _, table := range discovered {
			discoveredSet[table] = struct{}{}
		}

		candidates := discovered
		if len(database.IncludeTables) > 0 {
			candidates = database.IncludeTables
		}
		for _, table := range candidates {
			if len(database.IncludeTables) > 0 {
				if _, ok := discoveredSet[table]; !ok {
					continue
				}
			}
			if _, ok := excluded[table]; ok {
				continue
			}
			targetKey := targetTableKey{
				TargetName:     database.Target.Name,
				TargetHost:     database.Target.Host,
				TargetPort:     database.Target.Port,
				TargetUser:     database.Target.User,
				TargetDatabase: database.Target.Database,
				TargetTable:    table,
			}
			if existing, ok := plannedTargets[targetKey]; ok {
				return nil, fmt.Errorf(
					"duplicate target table %s.%s at %s:%d as %s: %s.%s.%s and %s.%s.%s both map to target table %s",
					database.Target.Name,
					database.Target.Database,
					database.Target.Host,
					database.Target.Port,
					database.Target.User,
					existing.SourceName,
					existing.SourceDatabase,
					existing.SourceTable,
					database.Source.Name,
					database.Source.Database,
					table,
					table,
				)
			}
			task := Task{
				SourceName:     database.Source.Name,
				SourceHost:     database.Source.Host,
				SourcePort:     database.Source.Port,
				SourceUser:     database.Source.User,
				SourcePassword: database.Source.Password,
				SourceDatabase: database.Source.Database,
				SourceTable:    table,
				TargetName:     database.Target.Name,
				TargetHost:     database.Target.Host,
				TargetPort:     database.Target.Port,
				TargetUser:     database.Target.User,
				TargetPassword: database.Target.Password,
				TargetDatabase: database.Target.Database,
			}
			plannedTargets[targetKey] = task
			tasks = append(tasks, task)
		}
	}
	return tasks, nil
}

func databaseKey(source DatabaseEndpoint) DatabaseKey {
	return DatabaseKey{
		SourceName: source.Name,
		SourceHost: source.Host,
		SourcePort: source.Port,
		SourceUser: source.User,
		Database:   source.Database,
	}
}
