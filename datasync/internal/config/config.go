package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultOutputDir   = "./runs"
	defaultParallelism = 1
	defaultMaxAttempts = 3
	defaultBackoff     = "2s"
	maxPort            = 65535
)

var envRefPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

type Config struct {
	MoDumpPath  string         `yaml:"mo_dump_path"`
	MySQLPath   string         `yaml:"mysql_path"`
	OutputDir   string         `yaml:"output_dir"`
	Parallelism int            `yaml:"parallelism"`
	Retry       RetryConfig    `yaml:"retry"`
	Databases   []DatabaseTask `yaml:"databases"`
}

type RetryConfig struct {
	MaxAttempts int           `yaml:"max_attempts"`
	BackoffText string        `yaml:"backoff"`
	Backoff     time.Duration `yaml:"-"`
}

type Endpoint struct {
	Name     string `yaml:"name"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type DatabaseEndpoint struct {
	Name     string `yaml:"name"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

type DatabaseTask struct {
	Source        DatabaseEndpoint `yaml:"source"`
	Target        DatabaseEndpoint `yaml:"target"`
	IncludeTables []string         `yaml:"include_tables"`
	ExcludeTables []string         `yaml:"exclude_tables"`
}

type rawConfig struct {
	MoDumpPath   string
	MySQLPath    string
	OutputDir    string
	OutputDirSet bool
	Parallelism  int
	ParallelSet  bool
	Retry        rawRetryConfig
	RetrySet     bool
	Databases    []DatabaseTask
}

type rawRetryConfig struct {
	MaxAttempts *int
	BackoffText string
	BackoffSet  bool
}

func (r *rawConfig) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("config must be a mapping")
	}
	if err := rejectDuplicateMappingKeys(value, ""); err != nil {
		return err
	}

	for i := 0; i < len(value.Content); i += 2 {
		key := value.Content[i]
		val := value.Content[i+1]
		switch key.Value {
		case "mo_dump_path":
			if err := val.Decode(&r.MoDumpPath); err != nil {
				return err
			}
		case "mysql_path":
			if err := val.Decode(&r.MySQLPath); err != nil {
				return err
			}
		case "output_dir":
			r.OutputDirSet = true
			if err := val.Decode(&r.OutputDir); err != nil {
				return err
			}
		case "parallelism":
			r.ParallelSet = true
			if val.Tag == "!!null" {
				r.Parallelism = 0
				continue
			}
			if err := val.Decode(&r.Parallelism); err != nil {
				return err
			}
		case "retry":
			r.RetrySet = true
			if val.Tag == "!!null" {
				return fmt.Errorf("retry must be a mapping")
			}
			if err := val.Decode(&r.Retry); err != nil {
				return err
			}
		case "databases":
			if err := decodeNodeStrict(val, &r.Databases); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%s is not a known field", key.Value)
		}
	}
	return nil
}

func decodeNodeStrict(node *yaml.Node, out any) error {
	data, err := yaml.Marshal(node)
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	return decoder.Decode(out)
}

func (r *rawRetryConfig) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode && value.Tag == "!!null" {
		return fmt.Errorf("retry must be a mapping")
	}
	if value.Kind != yaml.MappingNode {
		return fmt.Errorf("retry must be a mapping")
	}
	if err := rejectDuplicateMappingKeys(value, "retry."); err != nil {
		return err
	}

	for i := 0; i < len(value.Content); i += 2 {
		key := value.Content[i]
		val := value.Content[i+1]
		switch key.Value {
		case "max_attempts":
			var maxAttempts int
			if err := val.Decode(&maxAttempts); err != nil {
				return err
			}
			r.MaxAttempts = &maxAttempts
		case "backoff":
			r.BackoffSet = true
			if val.Tag == "!!null" {
				r.BackoffText = ""
				continue
			}
			var backoff string
			if err := val.Decode(&backoff); err != nil {
				return err
			}
			r.BackoffText = backoff
		default:
			return fmt.Errorf("retry.%s is not a known field", key.Value)
		}
	}
	return nil
}

func rejectDuplicateMappingKeys(value *yaml.Node, prefix string) error {
	seen := make(map[string]struct{}, len(value.Content)/2)
	for i := 0; i < len(value.Content); i += 2 {
		key := value.Content[i].Value
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate key %s%s", prefix, key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var raw rawConfig
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&raw); err != nil {
		return nil, err
	}

	cfg := materializeConfig(raw)
	if err := applyDefaultsAndValidate(&cfg, raw.OutputDirSet); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func materializeConfig(raw rawConfig) Config {
	cfg := Config{
		MoDumpPath: raw.MoDumpPath,
		MySQLPath:  raw.MySQLPath,
		OutputDir:  raw.OutputDir,
		Databases:  raw.Databases,
	}
	if !raw.ParallelSet {
		cfg.Parallelism = defaultParallelism
	} else {
		cfg.Parallelism = raw.Parallelism
	}
	if !raw.RetrySet || raw.Retry.MaxAttempts == nil {
		cfg.Retry.MaxAttempts = defaultMaxAttempts
	} else {
		cfg.Retry.MaxAttempts = *raw.Retry.MaxAttempts
	}
	if !raw.RetrySet || !raw.Retry.BackoffSet {
		cfg.Retry.BackoffText = defaultBackoff
	} else {
		cfg.Retry.BackoffText = raw.Retry.BackoffText
	}
	return cfg
}

func expandEnvRefs(value string) (string, error) {
	var missing string
	expanded := envRefPattern.ReplaceAllStringFunc(value, func(match string) string {
		if missing != "" {
			return match
		}
		name := envRefPattern.FindStringSubmatch(match)[1]
		value, ok := os.LookupEnv(name)
		if !ok {
			missing = name
			return match
		}
		return value
	})
	if missing != "" {
		return "", fmt.Errorf("environment variable %s is not set", missing)
	}
	return expanded, nil
}

func applyDefaultsAndValidate(cfg *Config, outputDirSet bool) error {
	if !outputDirSet {
		cfg.OutputDir = defaultOutputDir
	}

	if err := expandConfigEnvRefs(cfg); err != nil {
		return err
	}

	backoff, err := time.ParseDuration(cfg.Retry.BackoffText)
	if err != nil {
		return fmt.Errorf("retry.backoff: %w", err)
	}
	if backoff <= 0 {
		return fmt.Errorf("retry.backoff must be greater than 0")
	}
	cfg.Retry.Backoff = backoff

	return validate(cfg)
}

func expandConfigEnvRefs(cfg *Config) error {
	stringFields := []*string{
		&cfg.MoDumpPath,
		&cfg.MySQLPath,
		&cfg.OutputDir,
		&cfg.Retry.BackoffText,
	}

	for databaseIndex := range cfg.Databases {
		database := &cfg.Databases[databaseIndex]
		stringFields = append(stringFields,
			&database.Source.Name,
			&database.Source.Host,
			&database.Source.User,
			&database.Source.Password,
			&database.Source.Database,
			&database.Target.Name,
			&database.Target.Host,
			&database.Target.User,
			&database.Target.Password,
			&database.Target.Database,
		)

		for includeIndex := range database.IncludeTables {
			stringFields = append(stringFields, &database.IncludeTables[includeIndex])
		}
		for excludeIndex := range database.ExcludeTables {
			stringFields = append(stringFields, &database.ExcludeTables[excludeIndex])
		}
	}

	for _, field := range stringFields {
		expanded, err := expandEnvRefs(*field)
		if err != nil {
			return err
		}
		*field = expanded
	}
	return nil
}

func validate(cfg *Config) error {
	if cfg.MoDumpPath == "" {
		return fmt.Errorf("mo_dump_path is required")
	}
	if cfg.MySQLPath == "" {
		return fmt.Errorf("mysql_path is required")
	}
	if cfg.OutputDir == "" {
		return fmt.Errorf("output_dir is required")
	}
	if cfg.Parallelism < 1 {
		return fmt.Errorf("parallelism must be at least 1")
	}
	if cfg.Retry.MaxAttempts < 1 {
		return fmt.Errorf("retry.max_attempts must be at least 1")
	}
	if len(cfg.Databases) == 0 {
		return fmt.Errorf("databases must contain at least one database")
	}
	for databaseIndex, database := range cfg.Databases {
		prefix := fmt.Sprintf("databases[%d]", databaseIndex)
		if err := validateDatabaseEndpoint(prefix+".source", database.Source); err != nil {
			return err
		}
		if err := validateDatabaseEndpoint(prefix+".target", database.Target); err != nil {
			return err
		}
		if err := validateTableList(prefix+".include_tables", database.IncludeTables); err != nil {
			return err
		}
		if err := validateTableList(prefix+".exclude_tables", database.ExcludeTables); err != nil {
			return err
		}
	}
	return nil
}

func validateDatabaseEndpoint(prefix string, endpoint DatabaseEndpoint) error {
	if err := validateEndpoint(prefix, Endpoint{
		Name:     endpoint.Name,
		Host:     endpoint.Host,
		Port:     endpoint.Port,
		User:     endpoint.User,
		Password: endpoint.Password,
	}); err != nil {
		return err
	}
	if endpoint.Database == "" {
		return fmt.Errorf("%s.database is required", prefix)
	}
	return nil
}

func validateTableList(prefix string, tables []string) error {
	for tableIndex, table := range tables {
		if table == "" {
			return fmt.Errorf("%s[%d] must not be empty", prefix, tableIndex)
		}
	}
	return nil
}

func validateEndpoint(prefix string, endpoint Endpoint) error {
	if endpoint.Name == "" {
		return fmt.Errorf("%s.name is required", prefix)
	}
	if endpoint.Host == "" {
		return fmt.Errorf("%s.host is required", prefix)
	}
	if endpoint.Port < 1 {
		return fmt.Errorf("%s.port must be at least 1", prefix)
	}
	if endpoint.Port > maxPort {
		return fmt.Errorf("%s.port must be at most %d", prefix, maxPort)
	}
	if endpoint.User == "" {
		return fmt.Errorf("%s.user is required", prefix)
	}
	if endpoint.Password == "" {
		return fmt.Errorf("%s.password is required", prefix)
	}
	return nil
}
