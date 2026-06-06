package datasync

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

func (t Task) TargetEndpoint() Endpoint {
	return Endpoint{
		Name:     t.TargetName,
		Host:     t.TargetHost,
		Port:     t.TargetPort,
		User:     t.TargetUser,
		Password: t.TargetPassword,
	}
}

func BuildTasks(cfg *Config, tables map[DatabaseKey][]string) []Task {
	var tasks []Task
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
			tasks = append(tasks, Task{
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
			})
		}
	}
	return tasks
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
