package backup

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"sort"
	"strings"
)

const (
	Version0731 = "0731"
	Version     = Version0731
)

const (
	moMeta       = "mo_meta"
	configDir    = "config"
	taeDir       = "tae"
	hakeeperDir  = "hakeeper"
	HakeeperFile = "hk_data"
)

const (
	mb = 1024 * 1024
)

// Format :type,subtype,filename or dirname
const (
	TypePos              = 0
	SubTypePos           = 1
	FileNameOrDirNamePos = 2
)

type MetaType int

const (
	/**
	  backup_meta | mo_meta
	    ID        |
	    Version   | Version
	    Buildinfo | Buildinfo
	              | Launchconfig
	              | Tae
	              | Hakeeper
	*/
	TypeVersion MetaType = iota
	TypeBuildinfo
	TypeLaunchconfig
)

func (t MetaType) String() string {
	switch t {
	case TypeVersion:
		return "version"
	case TypeBuildinfo:
		return "buildinfo"
	case TypeLaunchconfig:
		return "launchconfig"
	default:
		return fmt.Sprintf("invalid type %d", t)
	}
}

// Meta of mo_meta same as the mo_br
type Meta struct {
	Typ               MetaType
	SubTyp            string
	FileNameOrDirName string

	//version
	Version string

	//build info
	Buildinfo string

	//launch config
	LaunchConfigFile string
}

func (m *Meta) String() string {
	line := m.CsvString()
	return strings.Join(line, ",")
}

func (m *Meta) CsvString() []string {
	format := make([]string, 0, 3)
	format = append(format, m.Typ.String(), m.SubTyp, m.FileNameOrDirName)
	switch m.Typ {
	case TypeVersion:
		format[SubTypePos] = m.Version
	case TypeBuildinfo:
		format[SubTypePos] = m.Buildinfo
	case TypeLaunchconfig:
		format[FileNameOrDirNamePos] = m.LaunchConfigFile
	}
	return format
}

type Metas struct {
	metas []*Meta
}

func NewMetas() *Metas {
	return &Metas{}
}

func (m *Metas) Append(meta *Meta) {
	m.metas = append(m.metas, meta)
}

func (m *Metas) AppendVersion(version string) {
	m.Append(&Meta{
		Typ:     TypeVersion,
		Version: version,
	})
}

func (m *Metas) AppendBuildinfo(info string) {
	m.Append(&Meta{
		Typ:       TypeBuildinfo,
		Buildinfo: info,
	})
}

func (m *Metas) AppendLaunchconfig(subTyp, file string) {
	m.Append(&Meta{
		Typ:              TypeLaunchconfig,
		SubTyp:           subTyp,
		LaunchConfigFile: file,
	})
}

func (m *Metas) orderTypes() []int {
	idx := make([]int, 0, len(m.metas))
	for i := range m.metas {
		idx = append(idx, i)
	}
	sort.Slice(idx, func(i, j int) bool {
		return m.metas[idx[i]].Typ < m.metas[idx[j]].Typ
	})
	return idx
}

func (m *Metas) CsvString() [][]string {
	lines := make([][]string, 0, len(m.metas))
	idx := m.orderTypes()
	for _, s := range idx {
		t := m.metas[s]
		lines = append(lines, t.CsvString())
	}
	return lines
}

func (m *Metas) String() string {
	lines := make([]string, 0, len(m.metas))
	idx := m.orderTypes()
	for _, s := range idx {
		t := m.metas[s]
		lines = append(lines, t.String())
	}
	return strings.Join(lines, "\n")
}

var (
	launchConfigPaths = make(map[string][]string)
)

const (
	CnConfig     = "cn"
	DnConfig     = "dn"
	LogConfig    = "log"
	ProxyConfig  = "proxy"
	LaunchConfig = "launch"
)

type Config struct {
	// Timestamp
	Timestamp types.TS

	// For General usage
	GeneralDir fileservice.FileService

	// For locating tae's storage fs
	SharedFs fileservice.FileService

	// For tae and hakeeper
	TaeDir fileservice.FileService

	// hakeeper client
	HAkeeper logservice.CNHAKeeperClient

	Metas *Metas
}

type s3Config struct {
	endpoint        string
	accessKeyId     string
	secretAccessKey string
	bucket          string
	filepath        string
	region          string
	compression     string
	roleArn         string
	provider        string
	externalId      string
	format          string
	jsonData        string
	isMinio         bool
}

type filesystemConfig struct {
	path string
}

type pathConfig struct {
	isS3   bool
	forETL bool
	s3Config
	filesystemConfig
}
