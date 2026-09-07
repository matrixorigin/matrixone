// Copyright 2021 Matrix Origin
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

package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"go.uber.org/zap"
)

type Nodes []Node

type Node struct {
	Mcpu      int
	Id        string             `json:"id"`
	Addr      string             `json:"address"`
	WorkState metadata.WorkState `json:"-"`
	//TODO::change RelData to Tombstoner, since only Tombstones ned to be serialized.
	Data  RelData
	CNCNT int32 // number of all cns
	CNIDX int32 // cn index , starts from 0
}

// QueryCandidate is a CN discovered before tenant and label pool resolution.
// Service keeps the control-plane metadata needed by pool policy; Mcpu is the
// CN-advertised CPU capacity normalized to at least one.
type QueryCandidate struct {
	Service metadata.CNService
	Mcpu    int
}

type QueryCandidates []QueryCandidate

// QueryCandidatePoolRequest contains statement/session constraints used to
// resolve the allowed CN pool. It deliberately contains no worker-selection
// policy such as subset size or ranking.
type QueryCandidatePoolRequest struct {
	IsInternal     bool
	Tenant         string
	Username       string
	CNLabel        map[string]string
	RequestedPool  string
	FallbackPolicy QueryPoolFallbackPolicy
}

type QueryPoolFallbackPolicy uint8

const (
	QueryPoolFallbackLegacyCompatible QueryPoolFallbackPolicy = iota
	QueryPoolFallbackStrict
)

func (p QueryPoolFallbackPolicy) Valid() bool {
	return p == QueryPoolFallbackLegacyCompatible || p == QueryPoolFallbackStrict
}

type QueryPoolResolution string

const (
	QueryPoolResolutionUnspecified      QueryPoolResolution = "unspecified"
	QueryPoolResolutionAllCompatible    QueryPoolResolution = "all-compatible"
	QueryPoolResolutionExactLabels      QueryPoolResolution = "exact-labels"
	QueryPoolResolutionNonAccountLabels QueryPoolResolution = "non-account-labels"
	QueryPoolResolutionSharedUnlabeled  QueryPoolResolution = "shared-unlabeled"
	QueryPoolResolutionPrivilegedAny    QueryPoolResolution = "privileged-any"
	QueryPoolResolutionNoMatch          QueryPoolResolution = "no-match"
)

type ResolvedQueryPool struct {
	Nodes             Nodes
	RequestedIdentity string
	Identity          string
	Resolution        QueryPoolResolution
	Fallback          bool
	FallbackReason    string
}

// QueryCandidateDiscoverer is an optional engine capability. Implementations
// return an unpooled cluster snapshot and must not apply tenant or label policy.
type QueryCandidateDiscoverer interface {
	DiscoverQueryCandidates(context.Context) (QueryCandidates, error)
}

// QueryCandidatePoolResolver is the matching optional capability that applies
// tenant and label policy to an already-discovered candidate snapshot.
// Implementations must treat candidates and request.CNLabel as read-only.
type QueryCandidatePoolResolver interface {
	ResolveQueryCandidatePool(context.Context, QueryCandidates, QueryCandidatePoolRequest) (ResolvedQueryPool, error)
}

func PlanDefToCstrDef(tableDef *plan.TableDef) *ConstraintDef {
	planDefs := tableDef.GetDefs()
	c := new(ConstraintDef)
	for _, def := range planDefs {
		switch defVal := def.GetDef().(type) {
		case *plan.TableDef_DefType_Properties:
			c.Cts = append(c.Cts, &StreamConfigsDef{
				Configs: defVal.Properties.GetProperties(),
			})
		}
	}

	if tableDef.Indexes != nil {
		c.Cts = append(c.Cts, &IndexDef{
			Indexes: tableDef.Indexes,
		})
	}

	if tableDef.Fkeys != nil {
		c.Cts = append(c.Cts, &ForeignKeyDef{
			Fkeys: tableDef.Fkeys,
		})
	}

	if tableDef.Pkey != nil {
		c.Cts = append(c.Cts, &PrimaryKeyDef{
			Pkey: tableDef.Pkey,
		})
	}

	if len(tableDef.RefChildTbls) > 0 {
		c.Cts = append(c.Cts, &RefChildTableDef{
			Tables: tableDef.RefChildTbls,
		})
	}

	return c
}

var PlanDefsToExeDefs = func(tableDef *plan.TableDef) ([]TableDef, *api.SchemaExtra, error) {
	planDefs := tableDef.GetDefs()
	var exeDefs []TableDef
	var propDef *PropertiesDef
	for _, def := range planDefs {
		switch defVal := def.GetDef().(type) {
		case *plan.TableDef_DefType_Properties:
			properties := make([]Property, len(defVal.Properties.GetProperties()))
			for i, p := range defVal.Properties.GetProperties() {
				properties[i] = Property{
					Key:   p.GetKey(),
					Value: p.GetValue(),
				}
			}
			propDef = &PropertiesDef{Properties: properties}
			exeDefs = append(exeDefs, propDef)
		}
	}

	if propDef == nil {
		propDef = &PropertiesDef{Properties: make([]Property, 0)}
		exeDefs = append(exeDefs, propDef)
	}
	extra := &api.SchemaExtra{
		FeatureFlag:    tableDef.FeatureFlag,
		AutoIncrOffset: tableDef.AutoIncrOffset,
		AutoIncrEpoch:  tableDef.AutoIncrEpoch,
		Checks:         tableDef.Checks,
		DefaultCharset: tableDef.DefaultCharset,
	}
	propDef.Properties = append(
		propDef.Properties,
		Property{
			Key: "schema_extra",
			ValueFactory: func() string {
				return string(api.MustMarshalTblExtra(extra))
			},
		},
	)

	if tableDef.ViewSql != nil {
		exeDefs = append(exeDefs, &ViewDef{
			View: tableDef.ViewSql.View,
		})
	}

	if tableDef.Partition != nil {
		bytes, err := tableDef.Partition.Marshal()
		if err != nil {
			return nil, nil, err
		}
		exeDefs = append(exeDefs, &PartitionDef{
			Partitioned: 1,
			Partition:   string(bytes),
		})
	}

	c := PlanDefToCstrDef(tableDef)
	if len(c.Cts) > 0 {
		exeDefs = append(exeDefs, c)
	}

	if tableDef.ClusterBy != nil {
		exeDefs = append(exeDefs, &ClusterByDef{
			Name: tableDef.ClusterBy.Name,
		})
	}

	return exeDefs, extra, nil
}

func PlanColsToExeCols(planCols []*plan.ColDef) []TableDef {
	exeCols := make([]TableDef, len(planCols))
	for i, col := range planCols {
		var alg compress.T
		switch col.Alg {
		case plan.CompressType_None:
			alg = compress.None
		case plan.CompressType_Lz4:
			alg = compress.Lz4
		}
		colTyp := col.GetTyp()
		exeTyp := types.NewWithCharset(
			types.T(colTyp.GetId()), colTyp.GetWidth(), colTyp.GetScale(), uint8(colTyp.GetCharset()),
		)
		exeCols[i] = &AttributeDef{
			Attr: Attribute{
				Name:          col.GetOriginCaseName(),
				Alg:           alg,
				Type:          exeTyp,
				Default:       planCols[i].GetDefault(),
				OnUpdate:      planCols[i].GetOnUpdate(),
				GeneratedCol:  col.GetGeneratedCol(),
				Primary:       col.GetPrimary(),
				Comment:       col.GetComment(),
				ClusterBy:     col.ClusterBy,
				AutoIncrement: col.Typ.GetAutoIncr(),
				IsHidden:      col.Hidden,
				Seqnum:        uint16(col.Seqnum),
				EnumVlaues:    colTyp.GetEnumvalues(),
			},
		}
	}
	return exeCols
}

// Attribute is a column
type Attribute struct {
	// IsHide whether the attribute is hidden or not
	IsHidden bool
	// IsRowId whether the attribute is rowid or not
	IsRowId bool
	// Column ID
	ID uint64
	// Name name of attribute, letter case: origin
	Name string
	// Alg compression algorithm
	Alg compress.T
	// Type attribute's type
	Type types.Type
	// DefaultExpr default value of this attribute
	Default *plan.Default
	// to update col when define in create table
	OnUpdate *plan.OnUpdate
	// Primary is primary key or not
	Primary bool
	// Clusterby means sort by this column
	ClusterBy bool
	// Comment of attribute
	Comment string
	// AutoIncrement is auto incr or not
	AutoIncrement bool
	// Seqnum, do not change during the whole lifetime of the table
	Seqnum uint16
	// EnumValues is for enum type
	EnumVlaues string
	// GeneratedCol is for generated (computed) columns
	GeneratedCol *plan.GeneratedCol
}

type PropertiesDef struct {
	Properties []Property
}

type Property struct {
	Key          string
	Value        string
	ValueFactory func() string
}

type ClusterByDef struct {
	Name string
}

type Statistics interface {
	// NOTE: Stats May indirectly access the file service
	Stats(ctx context.Context, sync bool) (*pb.StatsInfo, error)
	Rows(ctx context.Context) (uint64, error)
	Size(ctx context.Context, columnName string) (uint64, error)
}

type IndexTableDef struct {
	Typ      IndexT
	ColNames []string
	Name     string
}

type IndexT int

func (node IndexT) ToString() string {
	switch node {
	case ZoneMap:
		return "ZONEMAP"
	case BsiIndex:
		return "BSI"
	default:
		return "INVAILD"
	}
	//TODO: @arjun fix this later
	// Should this be same as secondary index algo type?
}

const (
	Empty IndexT = iota
	ZoneMap
	BsiIndex
)

type AttributeDef struct {
	Attr Attribute
}

type CommentDef struct {
	Comment string
}

type VersionDef struct {
	Version uint32
}

type PartitionDef struct {
	Partitioned int8
	Partition   string
}

type ViewDef struct {
	View string
}

type IndexDef struct {
	Indexes []*plan.IndexDef
}

type ForeignKeyDef struct {
	Fkeys []*plan.ForeignKeyDef
}

type PrimaryKeyDef struct {
	Pkey *plan.PrimaryKeyDef
}

type RefChildTableDef struct {
	Tables []uint64
}

type StreamConfigsDef struct {
	Configs []*plan.Property
}

type TableDef interface {
	tableDef()

	// ToPBVersion returns corresponding PB struct.
	ToPBVersion() TableDefPB
}

func (*CommentDef) tableDef()    {}
func (*VersionDef) tableDef()    {}
func (*PartitionDef) tableDef()  {}
func (*ViewDef) tableDef()       {}
func (*AttributeDef) tableDef()  {}
func (*IndexTableDef) tableDef() {}
func (*PropertiesDef) tableDef() {}
func (*ClusterByDef) tableDef()  {}
func (*ConstraintDef) tableDef() {}

func (def *CommentDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_CommentDef{
			CommentDef: def,
		},
	}
}

func (def *VersionDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_VersionDef{
			VersionDef: def,
		},
	}
}

func (def *PartitionDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_PartitionDef{
			PartitionDef: def,
		},
	}
}

func (def *ViewDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_ViewDef{
			ViewDef: def,
		},
	}
}

func (def *AttributeDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_AttributeDef{
			AttributeDef: def,
		},
	}
}

func (def *IndexTableDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_IndexTableDef{
			IndexTableDef: def,
		},
	}
}

func (def *PropertiesDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_PropertiesDef{
			PropertiesDef: def,
		},
	}
}

func (def *ClusterByDef) ToPBVersion() TableDefPB {
	return TableDefPB{
		Def: &TableDefPB_ClusterByDef{
			ClusterByDef: def,
		},
	}
}

func (def *ConstraintDef) ToPBVersion() TableDefPB {
	cts := make([]ConstraintPB, 0, len(def.Cts))
	for i := 0; i < len(def.Cts); i++ {
		cts = append(cts, def.Cts[i].ToPBVersion())
	}

	return TableDefPB{
		Def: &TableDefPB_ConstraintDefPB{
			ConstraintDefPB: &ConstraintDefPB{
				Cts: cts,
			},
		},
	}
}

func (def *TableDefPB) FromPBVersion() TableDef {
	if r := def.GetCommentDef(); r != nil {
		return r
	}
	if r := def.GetPartitionDef(); r != nil {
		return r
	}
	if r := def.GetViewDef(); r != nil {
		return r
	}
	if r := def.GetAttributeDef(); r != nil {
		return r
	}
	if r := def.GetIndexTableDef(); r != nil {
		return r
	}
	if r := def.GetPropertiesDef(); r != nil {
		return r
	}
	if r := def.GetClusterByDef(); r != nil {
		return r
	}
	if r := def.GetConstraintDefPB(); r != nil {
		return r.FromPBVersion()
	}
	panic("no corresponding type")
}

type ConstraintDef struct {
	Cts []Constraint
}

type ConstraintType int8

const (
	Index ConstraintType = iota
	RefChildTable
	ForeignKey
	PrimaryKey
	StreamConfig
)

type EngineType int8

const (
	Disttae EngineType = 0
	UNKNOWN EngineType = 2
)

func (def *ConstraintDef) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 0, def.marshalSize())
	for _, ct := range def.Cts {
		switch def := ct.(type) {
		case *IndexDef:
			data = append(data, byte(Index))
			data = appendConstraintUint64(data, uint64(len(def.Indexes)))
			for _, indexdef := range def.Indexes {
				data, err = appendConstraintProto(data, indexdef)
				if err != nil {
					return nil, err
				}
			}
		case *RefChildTableDef:
			data = append(data, byte(RefChildTable))
			data = appendConstraintUint64(data, uint64(len(def.Tables)))
			for _, tblId := range def.Tables {
				data = appendConstraintUint64(data, tblId)
			}

		case *ForeignKeyDef:
			data = append(data, byte(ForeignKey))
			data = appendConstraintUint64(data, uint64(len(def.Fkeys)))
			for _, fk := range def.Fkeys {
				data, err = appendConstraintProto(data, fk)
				if err != nil {
					return nil, err
				}
			}
		case *PrimaryKeyDef:
			data = append(data, byte(PrimaryKey))
			data, err = appendConstraintProto(data, def.Pkey)
			if err != nil {
				return nil, err
			}
		case *StreamConfigsDef:
			data = append(data, byte(StreamConfig))
			data = appendConstraintUint64(data, uint64(len(def.Configs)))
			for _, c := range def.Configs {
				data, err = appendConstraintProto(data, c)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return data, nil
}

type constraintProtoMarshaler interface {
	ProtoSize() int
	MarshalTo([]byte) (int, error)
}

func (def *ConstraintDef) marshalSize() int {
	size := 0
	for _, ct := range def.Cts {
		switch def := ct.(type) {
		case *IndexDef:
			size += 1 + 8
			for _, indexdef := range def.Indexes {
				size += 8 + indexdef.ProtoSize()
			}
		case *RefChildTableDef:
			size += 1 + 8 + 8*len(def.Tables)
		case *ForeignKeyDef:
			size += 1 + 8
			for _, fk := range def.Fkeys {
				size += 8 + fk.ProtoSize()
			}
		case *PrimaryKeyDef:
			size += 1 + 8 + def.Pkey.ProtoSize()
		case *StreamConfigsDef:
			size += 1 + 8
			for _, config := range def.Configs {
				size += 8 + config.ProtoSize()
			}
		}
	}
	return size
}

func appendConstraintUint64(data []byte, value uint64) []byte {
	start := len(data)
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(data[start:], value)
	return data
}

func appendConstraintProto(data []byte, message constraintProtoMarshaler) ([]byte, error) {
	size := message.ProtoSize()
	data = appendConstraintUint64(data, uint64(size))
	start := len(data)
	data = data[:start+size]
	written, err := message.MarshalTo(data[start:])
	if err != nil {
		return nil, err
	}
	if written != size {
		return nil, moerr.NewInternalErrorNoCtx("constraint protobuf size mismatch")
	}
	return data, nil
}

func (def *ConstraintDef) UnmarshalBinary(data []byte) error {
	l := 0
	var length uint64
	for l < len(data) {
		typ := ConstraintType(data[l])
		l += 1
		switch typ {
		case Index:
			length = binary.BigEndian.Uint64(data[l : l+8])
			l += 8
			indexes := make([]*plan.IndexDef, length)

			for i := 0; i < int(length); i++ {
				dataLength := binary.BigEndian.Uint64(data[l : l+8])
				l += 8
				indexdef := &plan.IndexDef{}
				err := indexdef.Unmarshal(data[l : l+int(dataLength)])
				if err != nil {
					return err
				}
				l += int(dataLength)
				indexes[i] = indexdef
			}
			def.Cts = append(def.Cts, &IndexDef{indexes})
		case RefChildTable:
			length = binary.BigEndian.Uint64(data[l : l+8])
			l += 8
			tables := make([]uint64, length)
			for i := 0; i < int(length); i++ {
				tblId := binary.BigEndian.Uint64(data[l : l+8])
				l += 8
				tables[i] = tblId
			}
			def.Cts = append(def.Cts, &RefChildTableDef{tables})

		case ForeignKey:
			length = binary.BigEndian.Uint64(data[l : l+8])
			l += 8
			fKeys := make([]*plan.ForeignKeyDef, length)

			for i := 0; i < int(length); i++ {
				dataLength := binary.BigEndian.Uint64(data[l : l+8])
				l += 8
				fKey := &plan.ForeignKeyDef{}
				err := fKey.Unmarshal(data[l : l+int(dataLength)])
				if err != nil {
					return err
				}
				l += int(dataLength)
				fKeys[i] = fKey
			}
			def.Cts = append(def.Cts, &ForeignKeyDef{fKeys})

		case PrimaryKey:
			length = binary.BigEndian.Uint64(data[l : l+8])
			l += 8
			pkey := &plan.PrimaryKeyDef{}
			err := pkey.Unmarshal(data[l : l+int(length)])
			if err != nil {
				return err
			}
			l += int(length)
			def.Cts = append(def.Cts, &PrimaryKeyDef{pkey})
		case StreamConfig:
			length = binary.BigEndian.Uint64(data[l : l+8])
			l += 8
			configs := make([]*plan.Property, length)

			for i := 0; i < int(length); i++ {
				dataLength := binary.BigEndian.Uint64(data[l : l+8])
				l += 8
				config := &plan.Property{}
				err := config.Unmarshal(data[l : l+int(dataLength)])
				if err != nil {
					return err
				}
				l += int(dataLength)
				configs[i] = config
			}
			def.Cts = append(def.Cts, &StreamConfigsDef{configs})
		}
	}
	return nil
}

func (def *ConstraintDefPB) FromPBVersion() *ConstraintDef {
	cts := make([]Constraint, 0, len(def.Cts))
	for i := 0; i < len(def.Cts); i++ {
		cts = append(cts, def.Cts[i].FromPBVersion())
	}
	return &ConstraintDef{
		Cts: cts,
	}
}

func (def *ConstraintPB) FromPBVersion() Constraint {
	if r := def.GetForeignKeyDef(); r != nil {
		return r
	}
	if r := def.GetPrimaryKeyDef(); r != nil {
		return r
	}
	if r := def.GetRefChildTableDef(); r != nil {
		return r
	}
	if r := def.GetIndexDef(); r != nil {
		return r
	}
	if r := def.GetStreamConfigsDef(); r != nil {
		return r
	}
	panic("no corresponding type")
}

// get the primary key definition in the constraint, and return null if there is no primary key
func (def *ConstraintDef) GetPrimaryKeyDef() *PrimaryKeyDef {
	for _, ct := range def.Cts {
		if ctVal, ok := ct.(*PrimaryKeyDef); ok {
			return ctVal
		}
	}
	return nil
}

type Constraint interface {
	constraint()

	// ToPBVersion returns corresponding PB struct.
	ToPBVersion() ConstraintPB
}

// TODO: UniqueIndexDef, SecondaryIndexDef will not be tabledef and need to be moved in Constraint to be able modified
func (*ForeignKeyDef) constraint()    {}
func (*PrimaryKeyDef) constraint()    {}
func (*RefChildTableDef) constraint() {}
func (*IndexDef) constraint()         {}
func (*StreamConfigsDef) constraint() {}

func (def *ForeignKeyDef) ToPBVersion() ConstraintPB {
	return ConstraintPB{
		Ct: &ConstraintPB_ForeignKeyDef{
			ForeignKeyDef: def,
		},
	}
}
func (def *PrimaryKeyDef) ToPBVersion() ConstraintPB {
	return ConstraintPB{
		Ct: &ConstraintPB_PrimaryKeyDef{
			PrimaryKeyDef: def,
		},
	}
}
func (def *RefChildTableDef) ToPBVersion() ConstraintPB {
	return ConstraintPB{
		Ct: &ConstraintPB_RefChildTableDef{
			RefChildTableDef: def,
		},
	}
}
func (def *IndexDef) ToPBVersion() ConstraintPB {
	return ConstraintPB{
		Ct: &ConstraintPB_IndexDef{
			IndexDef: def,
		},
	}
}

func (def *StreamConfigsDef) ToPBVersion() ConstraintPB {
	return ConstraintPB{
		Ct: &ConstraintPB_StreamConfigsDef{
			StreamConfigsDef: def,
		},
	}
}

type TombstoneType uint8

const (
	InvalidTombstoneData TombstoneType = iota
	TombstoneData
)

type DataCollectPolicy uint64

const (
	Policy_CollectCommittedInmemData = 1 << iota
	Policy_CollectUncommittedInmemData
	Policy_CollectCommittedPersistedData
	Policy_CollectUncommittedPersistedData
	Policy_CollectCommittedData   = Policy_CollectCommittedInmemData | Policy_CollectCommittedPersistedData
	Policy_CollectUncommittedData = Policy_CollectUncommittedInmemData | Policy_CollectUncommittedPersistedData
	Policy_CollectAllData         = Policy_CollectCommittedData | Policy_CollectUncommittedData
)

type TombstoneCollectPolicy uint64

const (
	Policy_CollectUncommittedTombstones = 1 << iota
	Policy_CollectCommittedTombstones
	Policy_CollectAllTombstones = Policy_CollectUncommittedTombstones | Policy_CollectCommittedTombstones
)

type TombstoneApplyPolicy uint64

const (
	Policy_SkipUncommitedInMemory = 1 << iota
	Policy_SkipCommittedInMemory
	Policy_SkipUncommitedS3
	Policy_SkipCommittedS3
)

const (
	Policy_CheckAll             = 0
	Policy_CheckCommittedS3Only = Policy_SkipUncommitedInMemory | Policy_SkipCommittedInMemory | Policy_SkipUncommitedS3
	Policy_CheckCommittedOnly   = Policy_SkipUncommitedInMemory | Policy_SkipUncommitedS3
	Policy_CheckUnCommittedOnly = Policy_SkipCommittedInMemory | Policy_SkipCommittedS3
	Policy_SkipAll              = Policy_SkipUncommitedInMemory | Policy_SkipCommittedInMemory | Policy_SkipUncommitedS3 | Policy_SkipCommittedS3
)

type Tombstoner interface {
	Type() TombstoneType
	HasAnyInMemoryTombstone() bool
	HasAnyTombstoneFile() bool

	String() string
	StringWithPrefix(string) string

	// false positive check, HasBlockTombstone will access FileService
	HasBlockTombstone(ctx context.Context, id *objectio.Blockid, fs fileservice.FileService) (bool, error)

	MarshalBinaryWithBuffer(w *bytes.Buffer) error
	UnmarshalBinary(buf []byte) error

	PrefetchTombstones(
		ctx context.Context,
		srvId string,
		fs fileservice.FileService,
		bid []objectio.Blockid,
	)

	// it applies the block related in-memory tombstones to the rowsOffset
	// `bid` is the block id
	// `rowsOffset` is the input rows offset to apply
	// `deleted` is the rows that are deleted from this apply
	// `left` is the rows that are left after this apply
	ApplyInMemTombstones(
		bid *types.Blockid,
		rowsOffset []int64,
		deleted *objectio.Bitmap,
	) (left []int64)

	// it applies the block related tombstones from the persisted tombstone file
	// to the rowsOffset
	ApplyPersistedTombstones(
		ctx context.Context,
		fs fileservice.FileService,
		snapshot *types.TS,
		bid *types.Blockid,
		rowsOffset []int64,
		deletedMask *objectio.Bitmap,
	) (left []int64, err error)

	// a.merge(b) => a = a U b
	// a and b must be sorted ascendingly
	// a.Type() must be equal to b.Type()
	Merge(other Tombstoner) error

	// in-memory tombstones must be sorted ascendingly
	// it should be called after all in-memory tombstones are added
	SortInMemory()
}

type RelDataType uint8

const (
	RelDataEmpty     RelDataType = 0
	RelDataBlockList RelDataType = 2
	RelDataObjList   RelDataType = 3
)

type RelData interface {
	// general interface

	GetType() RelDataType
	String() string
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(buf []byte) error
	AttachTombstones(tombstones Tombstoner) error
	GetTombstones() Tombstoner
	DataSlice(begin, end int) RelData

	BuildEmptyRelData(preAllocSize int) RelData
	DataCnt() int

	// for block info list
	Split(i int) []RelData
	GetBlockInfoSlice() objectio.BlockInfoSlice
	GetBlockInfo(i int) objectio.BlockInfo
	SetBlockInfo(i int, blk *objectio.BlockInfo)
	AppendBlockInfo(blk *objectio.BlockInfo)
	AppendBlockInfoSlice(objectio.BlockInfoSlice)
}

// ForRangeBlockInfo [begin, end)
func ForRangeBlockInfo(
	begin, end int,
	relData RelData,
	onBlock func(blk *objectio.BlockInfo) (bool, error)) error {
	if begin >= relData.DataCnt() {
		return nil
	}
	slice := relData.GetBlockInfoSlice()
	slice = slice.Slice(begin, end)
	sliceLen := slice.Len()

	for i := 0; i < sliceLen; i++ {
		if ok, err := onBlock(slice.Get(i)); !ok || err != nil {
			return err
		}
	}

	return nil
}

type DataState uint8

const (
	InMem DataState = iota
	Persisted
	End
)

type DataSourceType uint8

const (
	GeneralLocalDataSource DataSourceType = iota
	ShardingLocalDataSource
	ShardingRemoteDataSource
)

type DataSource interface {
	Next(
		ctx context.Context,
		cols []string,
		types []types.Type,
		seqNums []uint16,
		pkSeqNum int32,
		memFilter any,
		mp *mpool.MPool,
		bat *batch.Batch,
	) (*objectio.BlockInfo, DataState, error)

	ApplyTombstones(
		ctx context.Context,
		bid *objectio.Blockid,
		rowsOffset []int64,
		applyPolicy TombstoneApplyPolicy,
	) ([]int64, error)

	GetTombstones(
		ctx context.Context, bid *objectio.Blockid,
	) (deletedRows objectio.Bitmap, err error)

	SetOrderBy(orderby []*plan.OrderBySpec)
	GetOrderBy() []*plan.OrderBySpec

	SetFilterZM(zm objectio.ZoneMap)

	Close()
	String() string
}

type Ranges interface {
	GetBytes(i int) []byte

	Len() int

	Append([]byte)

	Size() int

	SetBytes([]byte)

	GetAllBytes() []byte

	Slice(i, j int) []byte
}

var _ Ranges = (*objectio.BlockInfoSlice)(nil)

type ChangesHandle_Hint int

const (
	ChangesHandle_Snapshot ChangesHandle_Hint = iota
	ChangesHandle_Tail_wip
	ChangesHandle_Tail_done
)

type ChangesHandle interface {
	Next(ctx context.Context, mp *mpool.MPool) (data *batch.Batch, tombstone *batch.Batch, hint ChangesHandle_Hint, err error)
	Close() error
}

type RangesShuffleParam struct {
	// these are for shuffle objects
	Node      *plan.Node
	CNCNT     int32 // number of all cns
	CNIDX     int32 // cn index , starts from 0
	IsLocalCN bool
	// ShuffleByObjectID assigns IVF persisted and appendable objects to the
	// same physical CN owner.
	ShuffleByObjectID  bool
	ShuffleRangeUint64 []uint64
	ShuffleRangeInt64  []int64
	Init               bool
}

type RangesParam struct {
	BlockFilters       []*plan.Expr //Slice of expressions used to filter zonemap
	PreAllocBlocks     int          //estimated count of blocks
	TxnOffset          int          //Transaction offset used to specify the starting position for reading data.
	Policy             DataCollectPolicy
	Rsp                *RangesShuffleParam
	DontSupportRelData bool
}

var DefaultRangesParam RangesParam = RangesParam{
	BlockFilters:       nil,
	PreAllocBlocks:     2,
	TxnOffset:          0,
	Policy:             Policy_CollectAllData,
	DontSupportRelData: true,
}

// Relation is bound to the transaction operator used to open its Database.
type Relation interface {
	Statistics

	Ranges(context.Context, RangesParam) (RelData, error)

	CollectTombstones(ctx context.Context, txnOffset int, policy TombstoneCollectPolicy) (Tombstoner, error)

	// StarCount returns the total number of visible rows at the current transaction snapshot.
	// Optimized for COUNT(*) queries by using metadata (total rows - deleted rows)
	// instead of scanning data blocks.
	StarCount(ctx context.Context) (uint64, error)

	// EstimateCommittedTombstoneCount returns an estimated count of committed tombstone rows.
	// This is very lightweight (only reads metadata, no S3 I/O) and can be used to decide
	// whether to use StarCount optimization.
	// Returns an upper bound estimate (includes duplicates and invisible data object references).
	EstimateCommittedTombstoneCount(ctx context.Context) (int, error)

	CollectChanges(ctx context.Context, from, to types.TS, skipDeletes bool, mp *mpool.MPool) (ChangesHandle, error)

	CollectObjectList(ctx context.Context, from, to types.TS, bat *batch.Batch, mp *mpool.MPool) error

	TableDefs(context.Context) ([]TableDef, error)

	GetExtraInfo() *api.SchemaExtra

	// Get complete tableDef information, including columns, constraints, partitions, version, comments, etc
	GetTableDef(context.Context) *plan.TableDef
	CopyTableDef(context.Context) *plan.TableDef

	GetPrimaryKeys(context.Context) ([]*Attribute, error)

	// Note: Write Will access Fileservice
	Write(context.Context, *batch.Batch) error

	// Delete(context.Context, *vector.Vector, string) error
	Delete(context.Context, *batch.Batch, string) error

	AddTableDef(context.Context, TableDef) error
	DelTableDef(context.Context, TableDef) error

	// only ConstraintDef can be modified
	UpdateConstraint(context.Context, *ConstraintDef) error

	AlterTable(context.Context, *ConstraintDef, []*api.AlterTableReq) error

	// Support renaming tables within explicit transactions (CN worspace)
	TableRenameInTxn(ctx context.Context, constraint [][]byte) error

	GetTableID(context.Context) uint64

	// GetTableName returns the name of the table.
	GetTableName() string

	GetDBID(context.Context) uint64

	// Note: Write Will access Fileservice
	BuildReaders(
		ctx context.Context,
		proc any,
		expr *plan.Expr,
		relData RelData,
		num int,
		txnOffset int,
		orderBy bool,
		policy TombstoneApplyPolicy,
		filterHint FilterHint,
	) ([]Reader, error)

	BuildShardingReaders(
		ctx context.Context,
		proc any,
		expr *plan.Expr,
		relData RelData,
		num int,
		txnOffset int,
		orderBy bool,
		policy TombstoneApplyPolicy,
	) ([]Reader, error)

	TableColumns(ctx context.Context) ([]*Attribute, error)

	//max and min values
	MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error)

	GetEngineType() EngineType

	GetProcess() any

	// Note: GetColumMetadataScanInfo Will access Fileservice
	GetColumMetadataScanInfo(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error)

	// PrimaryKeysMayBeModified reports whether any rows with any primary keys in keyVector was modified during `from` to `to`
	// If not sure, returns true
	// Initially added for implementing locking rows by primary keys
	PrimaryKeysMayBeModified(ctx context.Context, from types.TS, to types.TS, batch *batch.Batch, pkIndex, partitionIndex int32) (bool, error)

	PrimaryKeysMayBeUpserted(ctx context.Context, from types.TS, to types.TS, batch *batch.Batch, pkIndex int32) (bool, error)

	ApproxObjectsNum(ctx context.Context) int
	MergeObjects(ctx context.Context, objstats []objectio.ObjectStats, targetObjSize uint32) (*api.MergeCommitEntry, error)
	GetNonAppendableObjectStats(ctx context.Context) ([]objectio.ObjectStats, error)

	// GetFlushTS returns the flush timestamp of the relation.
	GetFlushTS(ctx context.Context) (types.TS, error)

	// Reset rebinds an exclusively owned relation handle to op. Reset must not
	// be called on a relation shared by multiple operators.
	Reset(op client.TxnOperator) error
}

// RelationHandleFactory is implemented by engines whose cached relations are
// shared and therefore cannot be reset directly. NewRelationHandle returns an
// exclusively owned, reusable handle over the shared relation.
type RelationHandleFactory interface {
	NewRelationHandle() Relation
}

// NewRelationHandle returns an exclusively owned handle when the engine
// supports one. Engines with immutable or already-exclusive relations may
// return the relation itself by not implementing RelationHandleFactory.
func NewRelationHandle(rel Relation) Relation {
	if factory, ok := rel.(RelationHandleFactory); ok {
		return factory.NewRelationHandle()
	}
	return rel
}

type BaseReader interface {
	Close() error
	Read(context.Context, []string, *plan.Expr, *mpool.MPool, *batch.Batch) (bool, error)
}

type Reader interface {
	BaseReader
	SetOrderBy([]*plan.OrderBySpec)
	GetOrderBy() []*plan.OrderBySpec
	SetIndexParam(*plan.IndexReaderParam)
	SetFilterZM(objectio.ZoneMap)
	//SetScanType()
}

// ExplainDiagnosticReader is an optional Reader capability for execution
// details that must reach EXPLAIN ANALYZE. TakeExplainDiagnostics transfers
// ownership to the caller and must not return the same diagnostic twice.
type ExplainDiagnosticReader interface {
	TakeExplainDiagnostics() []*plan.Query
}

// ReaderFilterResult describes which rows survived a ReaderFilter. Sels must
// contain sorted, unique positions in the callback's input batch, and its
// length must equal the filtered batch row count. Sels is borrowed from the
// callback and is only valid until the next callback. When All is true, every
// row survived, the callback must not change the row count, and Sels is ignored.
type ReaderFilterResult struct {
	Sels []int64
	All  bool
}

// ReaderFilter evaluates a residual predicate over the columns listed in
// loadedColumns. loadedColumns contains positions in the full output schema;
// nil means every output column is already loaded and the row mapping is not
// consumed. The callback must shrink the loaded vectors and update bat.RowCount
// when only a subset survives. Readers invoke the callback synchronously and
// must finish consuming its result before ReadWithFilter returns.
type ReaderFilter func(
	bat *batch.Batch,
	loadedColumns []int,
) (ReaderFilterResult, error)

// LateMaterializationReader is an optional Reader capability. It reads the
// early columns, applies filter, and materializes the remaining columns only
// for surviving persisted rows. Readers must fall back to an eager read for
// data sources that cannot be revisited, such as in-memory workspace data.
type LateMaterializationReader interface {
	ReadWithFilter(
		ctx context.Context,
		cols []string,
		earlyColumns []int,
		filter ReaderFilter,
		mp *mpool.MPool,
		outBatch *batch.Batch,
	) (isEnd bool, err error)
}

type Database interface {
	Relations(context.Context) ([]string, error)
	Relation(context.Context, string, any) (Relation, error)
	RelationExists(context.Context, string, any) (bool, error)

	Delete(context.Context, string) error
	Create(context.Context, string, []TableDef) error // Create Table - (name, table define)
	GetDatabaseId(context.Context) string
	IsSubscription(context.Context) bool
	GetCreateSql(context.Context) string
}

type LogtailEngine interface {
	// TryToSubscribeTable tries to subscribe a table.
	TryToSubscribeTable(context.Context, uint64, uint64, uint64, string, string) error
	// UnsubscribeTable unsubscribes a table from logtail client.
	UnsubscribeTable(context.Context, uint64, uint64, uint64) error
}

type Engine interface {
	// LogtailEngine has some actions for logtail.
	LogtailEngine

	// transaction interface
	New(ctx context.Context, op client.TxnOperator) error

	// Delete deletes a database
	Delete(ctx context.Context, databaseName string, op client.TxnOperator) error

	// Create creates a database
	Create(ctx context.Context, databaseName string, op client.TxnOperator) error

	// Databases returns all database names
	Databases(ctx context.Context, op client.TxnOperator) (databaseNames []string, err error)

	// Database creates a handle for a database
	Database(ctx context.Context, databaseName string, op client.TxnOperator) (Database, error)

	// Nodes returns all nodes for worker jobs. isInternal, tenant, cnLabel are
	// used to filter CN servers.
	Nodes(isInternal bool, tenant string, username string, cnLabel map[string]string) (cnNodes Nodes, err error)

	// Hints returns hints of engine features
	// return value should not be cached
	// since implementations may update hints after engine had initialized
	Hints() Hints

	BuildBlockReaders(
		ctx context.Context,
		proc any,
		ts timestamp.Timestamp,
		expr *plan.Expr,
		def *plan.TableDef,
		relData RelData,
		num int,
		filterHint ...FilterHint) ([]Reader, error)

	// Get database name & table name by table id
	GetNameById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, err error)

	// Get relation by table id
	GetRelationById(ctx context.Context, op client.TxnOperator, tableId uint64) (dbName string, tblName string, rel Relation, err error)

	// AllocateIDByKey allocate a globally unique ID by key.
	AllocateIDByKey(ctx context.Context, key string) (uint64, error)

	// Stats returns the stats info of the key.
	// If sync is true, wait for the stats info to be updated, else,
	// just return nil if the current stats info has not been initialized.
	Stats(ctx context.Context, key pb.StatsInfoKey, sync bool) *pb.StatsInfo

	// true if the prefetch is received, false if the prefetch is rejected
	PrefetchTableMeta(ctx context.Context, key pb.StatsInfoKey) bool

	GetMessageCenter() any

	GetService() string

	LatestLogtailAppliedTime() timestamp.Timestamp
}

// TableVersionedStats is an optional engine capability for readers that know
// the table definition used by their plan. Implementations must not return
// schema-bound statistics collected for another definition version. It is
// optional so engines and mocks that expose only metadata-derived statistics
// keep the existing Engine contract.
type TableVersionedStats interface {
	StatsAtTableVersion(
		ctx context.Context,
		key pb.StatsInfoKey,
		sync bool,
		tableDefVersion uint32,
	) *pb.StatsInfo
}

// RemoteStatsExporter is an optional engine capability for serving statistics
// to another CN. Unlike a local unversioned Stats reader, the remote caller
// cannot prove which table-definition version it will use. Implementations
// must therefore reject schema-bound statistics rather than serialize them.
type RemoteStatsExporter interface {
	StatsForRemote(ctx context.Context, key pb.StatsInfoKey) *pb.StatsInfo
}

// StatsRefreshOptions carries statistics that the statement computed from a
// table-wide scan. Object metadata remains the source of all fields not
// present here.
type StatsRefreshOptions struct {
	// TableDefVersion is the schema version that owned the table-wide
	// observation. It is required whenever TableRowCount or ColumnNDVs carries
	// an observation. The engine rejects it if the current physical table has
	// crossed a schema boundary, preventing an old column value from being
	// applied to a dropped-and-recreated column with the same name.
	TableDefVersion *uint32

	// TableRowCount is the exact row count observed by the same table-wide scan
	// as ColumnNDVs. Nil leaves the object-metadata estimate unchanged.
	TableRowCount *float64

	// ColumnNDVs maps canonical column names to table-wide approximate distinct
	// counts. The engine validates the names and values, caps them at the
	// effective table row count, and applies them before publishing the new
	// statistics object.
	ColumnNDVs map[string]float64
}

// StatsRefresher is an optional engine capability for statements that define
// a synchronous statistics-publication boundary, such as ANALYZE TABLE.
// Implementations must not return until Stats() can observe the returned
// statistics on the local engine instance.
type StatsRefresher interface {
	RefreshTableStats(ctx context.Context, key pb.StatsInfoKey) (*pb.StatsInfo, error)
}

// StatsRefresherWithOptions extends StatsRefresher without breaking engines
// that implement the original synchronous refresh capability.
type StatsRefresherWithOptions interface {
	StatsRefresher
	RefreshTableStatsWithOptions(
		ctx context.Context,
		key pb.StatsInfoKey,
		options StatsRefreshOptions,
	) (*pb.StatsInfo, error)
}

// AnalyzeTableRequest is the storage-facing contract for a manual ANALYZE
// collection. The relation owns snapshot visibility and physical range
// selection; callers provide only bounded policy inputs and resolved columns.
type AnalyzeTableRequest struct {
	Process           any
	Columns           []string
	FullScan          bool
	Seed              [32]byte
	TargetRows        uint64
	MinBlocks         uint64
	MaxBlocks         uint64
	MaxStrata         uint32
	MaxDistinctValues uint64
	ColumnsPerPass    uint32
}

// AnalyzeTableResult contains the StatsInfo compatibility adapter and explicit
// collection diagnostics. The relation never publishes this result itself.
type AnalyzeTableResult struct {
	Stats             *pb.StatsInfo
	Mode              string
	Coverage          string
	PopulationRows    uint64
	PopulationExact   bool
	PopulationBlocks  uint64
	SampleRows        uint64
	SampleBlocks      uint64
	SampleBytes       uint64
	ColumnsAnalyzed   uint32
	SampleNumerator   uint64
	SampleDenominator uint64
}

// AnalyzableRelation is optional so non-disttae engines and existing relation
// mocks are not forced to implement a storage-specific maintenance operation.
type AnalyzableRelation interface {
	AnalyzeTable(ctx context.Context, request AnalyzeTableRequest) (*AnalyzeTableResult, error)
}

// AnalyzedStatsPublisher owns the publication boundary after successful data
// collection. Durable publication can evolve behind this same capability.
type AnalyzedStatsPublisher interface {
	PublishAnalyzedStats(
		ctx context.Context,
		key pb.StatsInfoKey,
		tableDefVersion uint32,
		stats *pb.StatsInfo,
	) (*pb.StatsInfo, error)
}

// LogtailReadBarrier is an optional engine capability that establishes a
// linearizable read boundary against the TN commit/logtail publication order.
// On success, all commits completed before the boundary are visible through
// this local engine instance and frontier is the exact applied logtail target.
type LogtailReadBarrier interface {
	AcquireLogtailReadBarrier(ctx context.Context) (frontier timestamp.Timestamp, err error)
}

type VectorPool interface {
	PutBatch(bat *batch.Batch)
	GetVector(typ types.Type) *vector.Vector
}

// CatalogCacheGCer is an optional interface for engines that support
// on-demand GC of the in-memory catalog cache.
type CatalogCacheGCer interface {
	GCCatalogCache(ctx context.Context, ago time.Duration) error
}

type Hints struct {
	CommitOrRollbackTimeout time.Duration
}

// AutoIncrEpochFenceSupporter is implemented by transaction workspaces whose
// target TN snapshot can prove that every target enforces AUTO_INCREMENT
// allocator epochs.
type AutoIncrEpochFenceSupporter interface {
	SupportsAutoIncrEpochFence() bool
}

// SupportsAutoIncrEpochFence fails closed for legacy and unknown workspaces.
func SupportsAutoIncrEpochFence(workspace client.Workspace) bool {
	supporter, ok := workspace.(AutoIncrEpochFenceSupporter)
	return ok && supporter.SupportsAutoIncrEpochFence()
}

// TxnSupportsAutoIncrEpochFence fails closed when the transaction or its
// workspace cannot prove that every target TN enforces allocator epochs.
func TxnSupportsAutoIncrEpochFence(txn client.TxnOperator) bool {
	return txn != nil && SupportsAutoIncrEpochFence(txn.GetWorkspace())
}

// EntireEngine is a wrapper for Engine to support temporary table
type EntireEngine struct {
	Engine Engine // original engine
}

type forceBuildRemoteDSConfig struct {
	sync.Mutex
	force  bool
	tblIds []uint64
}

var forceBuildRemoteDS forceBuildRemoteDSConfig

type forceShuffleReaderConfig struct {
	sync.Mutex
	force  bool
	tblIds []uint64
	blkCnt int
}

var forceShuffleReader forceShuffleReaderConfig

type prefetchOnSubscribedConfig struct {
	sync.RWMutex
	overridden bool
	regexps    []*regexp.Regexp
}

var prefetchOnSubscribed prefetchOnSubscribedConfig

func SetForceBuildRemoteDS(force bool, tbls []string) {
	forceBuildRemoteDS.Lock()
	defer forceBuildRemoteDS.Unlock()

	forceBuildRemoteDS.tblIds = make([]uint64, len(tbls))
	for i, tbl := range tbls {
		id, err := strconv.Atoi(tbl)
		if err != nil {
			logutil.Errorf("SetForceBuildRemoteDS: invalid table id %s", tbl)
			return
		}

		forceBuildRemoteDS.tblIds[i] = uint64(id)
	}

	forceBuildRemoteDS.force = force
}

func GetForceBuildRemoteDS() (bool, []uint64) {
	forceBuildRemoteDS.Lock()
	defer forceBuildRemoteDS.Unlock()

	return forceBuildRemoteDS.force, forceBuildRemoteDS.tblIds
}

func SetForceShuffleReader(force bool, tbls []string, blkCnt int) {
	forceShuffleReader.Lock()
	defer forceShuffleReader.Unlock()

	forceShuffleReader.tblIds = make([]uint64, len(tbls))
	for i, tbl := range tbls {
		id, err := strconv.Atoi(tbl)
		if err != nil {
			logutil.Errorf("SetForceBuildRemoteDS: invalid table id %s", tbl)
			return
		}

		forceShuffleReader.tblIds[i] = uint64(id)
	}

	forceShuffleReader.force = force
	forceShuffleReader.blkCnt = blkCnt
}

func GetForceShuffleReader() (bool, []uint64, int) {
	forceShuffleReader.Lock()
	defer forceShuffleReader.Unlock()

	return forceShuffleReader.force, forceShuffleReader.tblIds, forceShuffleReader.blkCnt
}

func SetPrefetchOnSubscribed(patterns []string) error {
	if patterns == nil {
		prefetchOnSubscribed.Lock()
		prefetchOnSubscribed.overridden = false
		prefetchOnSubscribed.regexps = nil
		prefetchOnSubscribed.Unlock()
		return nil
	}

	regexps := make([]*regexp.Regexp, 0, len(patterns))
	for _, pattern := range patterns {
		r, err := regexp.Compile(pattern)
		if err != nil {
			return moerr.NewInternalErrorNoCtxf("compile pattern %q: %v", pattern, err)
		}
		regexps = append(regexps, r)
	}

	logutil.Info("Set-Prefetch-On-Subscribed-By-MO-CTL",
		zap.Strings("patterns", patterns),
	)

	prefetchOnSubscribed.Lock()
	prefetchOnSubscribed.regexps = regexps
	prefetchOnSubscribed.overridden = true
	prefetchOnSubscribed.Unlock()
	return nil
}

func GetPrefetchOnSubscribed() (bool, []*regexp.Regexp) {
	prefetchOnSubscribed.RLock()
	defer prefetchOnSubscribed.RUnlock()

	if !prefetchOnSubscribed.overridden {
		return false, nil
	}

	regexps := make([]*regexp.Regexp, len(prefetchOnSubscribed.regexps))
	copy(regexps, prefetchOnSubscribed.regexps)
	return true, regexps
}

// MembershipFilter is a membership filter over the indexed primary-key values
// (fulltext calls this PK doc_id) used to prune an index scan to the candidate
// rows that pass the surrounding relational predicate. It is implemented in
// pkg/common/docfilter by an exact set (dense cbitmap / sparse Sorted64) for
// integer PKs and by a CBloomFilter (approximate) for non-integer PKs.
//
// This is the CONSUMER (probe) view, so it deliberately omits Share() — a plain
// *bloomfilter.CBloomFilter satisfies it directly. The PRODUCER superset is
// docfilter.MembershipFilter, which adds Share() and is assignable to this
// interface (enforced by a compile-time assertion in package disttae, where
// both packages are imported). Keep the shared method set here as the single
// source of truth; docfilter's interface only adds to it.
type MembershipFilter interface {
	// Test reports whether the raw fixed bytes of a single key may be present.
	Test(data []byte) bool
	// TestVector tests every row of a key vector, invoking cb(exist, isnull, row).
	TestVector(v *vector.Vector, cb func(bool, bool, int)) []uint8
	// Valid reports whether the filter is usable.
	Valid() bool
	// Exact reports whether membership is exact (a bitset, no false positives)
	// rather than approximate (a bloom filter). Callers can skip downstream
	// re-verification when this is true.
	Exact() bool
	// Free releases any resources held by the filter.
	Free()
}

type FilterHint struct {
	Must                  bool
	MembershipFilterBytes []byte
	BF                    MembershipFilter
}
