// Copyright 2026 Matrix Origin
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

package dml

import (
	"context"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergref "github.com/matrixorigin/matrixone/pkg/iceberg/ref"
)

type Operation string

const (
	OperationDelete    Operation = "delete"
	OperationUpdate    Operation = "update"
	OperationMerge     Operation = "merge"
	OperationOverwrite Operation = "overwrite"
)

type TableMode string

const (
	TableModeMergeOnRead TableMode = "merge_on_read"
	TableModeCopyOnWrite TableMode = "copy_on_write"
)

type ActionKind string

const (
	ActionAppendData        ActionKind = "append_data"
	ActionAddEqualityDelete ActionKind = "add_equality_delete"
	ActionAddPositionDelete ActionKind = "add_position_delete"
	ActionRewriteDataFile   ActionKind = "rewrite_data_file"
	ActionDeleteDataFile    ActionKind = "delete_data_file"
)

type CommitBase struct {
	Namespace           api.Namespace
	Table               string
	TargetRef           string
	TargetRefType       string
	AllowTagMove        bool
	CatalogCapabilities api.CatalogCapabilities
	BaseSnapshotID      int64
	TableUUID           string
	BaseSchemaID        int
	BaseSpecID          int
	IdempotencyKey      string
	StatementID         string
	Summary             map[string]string
}

type Action struct {
	Kind             ActionKind
	File             api.DataFile
	DeleteFile       api.DataFile
	ReplacedFile     api.DataFile
	ReplacementFiles []api.DataFile
	Reason           string
}

type ActionStream struct {
	Operation Operation
	Base      CommitBase
	Actions   []Action
	Profile   Profile
}

// CommitIntent is the DML planner output before Iceberg manifest materialization.
// It is not a REST catalog CommitAttempt: data/delete file actions must still be
// written into data/delete manifests and a snapshot update by the write layer.
type CommitIntent struct {
	Requirements   []api.CommitRequirement
	Actions        []Action
	Summary        map[string]string
	IdempotencyKey string
	BaseSnapshotID int64
	BaseSchemaID   int
	BaseSpecID     int
	TargetRef      string
	TargetRefType  string
	Profile        Profile
}

type Profile struct {
	Operation            Operation
	MatchedRows          int64
	AddedDataFiles       int
	AddedDeleteFiles     int
	DeletedDataFiles     int
	RewrittenDataFiles   int
	EqualityDeleteFiles  int
	PositionDeleteFiles  int
	CopyOnWriteFallbacks int
	AdapterName          string
	UsedAdapter          bool
}

type DeleteTarget struct {
	DataFile           api.DataFile
	MatchedRows        int64
	EqualityFieldIDs   []int
	PredicateStable    bool
	HasRowOrdinal      bool
	EqualityDeleteFile api.DataFile
	PositionDeleteFile api.DataFile
	ReplacementFiles   []api.DataFile
}

type DeleteRequest struct {
	Base    CommitBase
	Mode    TableMode
	Targets []DeleteTarget
}

type UpdateTarget struct {
	DeleteTarget
	ReplacementFiles []api.DataFile
}

type UpdateRequest struct {
	Base             CommitBase
	Mode             TableMode
	Targets          []UpdateTarget
	AppendedDataFile []api.DataFile
}

type MergeRequest struct {
	Base              CommitBase
	Mode              TableMode
	MatchedDeletes    []DeleteTarget
	MatchedUpdates    []UpdateTarget
	UnmatchedAppends  []api.DataFile
	MatchedDeleteRows int64
}

type OverwriteScope string

const (
	OverwriteTable     OverwriteScope = "table"
	OverwritePartition OverwriteScope = "partition"
)

type OverwriteRequest struct {
	Base              CommitBase
	Scope             OverwriteScope
	Partition         map[string]any
	AffectedDataFiles []api.DataFile
	ReplacementFiles  []api.DataFile
}

type RowDeltaAdapter interface {
	Name() string
	SupportsRowDelta() bool
	BuildDelete(ctx context.Context, req DeleteRequest) (*ActionStream, bool, error)
	BuildUpdate(ctx context.Context, req UpdateRequest) (*ActionStream, bool, error)
	BuildMerge(ctx context.Context, req MergeRequest) (*ActionStream, bool, error)
}

type Planner struct {
	Adapter RowDeltaAdapter
}

func (p Planner) PlanDelete(ctx context.Context, req DeleteRequest) (*ActionStream, error) {
	if stream, ok, err := p.tryAdapterDelete(ctx, req); ok || err != nil {
		return stream, err
	}
	return NativePlanner{}.PlanDelete(ctx, req)
}

func (p Planner) PlanUpdate(ctx context.Context, req UpdateRequest) (*ActionStream, error) {
	if stream, ok, err := p.tryAdapterUpdate(ctx, req); ok || err != nil {
		return stream, err
	}
	return NativePlanner{}.PlanUpdate(ctx, req)
}

func (p Planner) PlanMerge(ctx context.Context, req MergeRequest) (*ActionStream, error) {
	if stream, ok, err := p.tryAdapterMerge(ctx, req); ok || err != nil {
		return stream, err
	}
	return NativePlanner{}.PlanMerge(ctx, req)
}

func (p Planner) tryAdapterDelete(ctx context.Context, req DeleteRequest) (*ActionStream, bool, error) {
	if p.Adapter == nil || !p.Adapter.SupportsRowDelta() {
		return nil, false, nil
	}
	stream, ok, err := p.Adapter.BuildDelete(ctx, req)
	if stream != nil {
		stream.Profile.AdapterName = p.Adapter.Name()
		stream.Profile.UsedAdapter = ok
	}
	return stream, ok, err
}

func (p Planner) tryAdapterUpdate(ctx context.Context, req UpdateRequest) (*ActionStream, bool, error) {
	if p.Adapter == nil || !p.Adapter.SupportsRowDelta() {
		return nil, false, nil
	}
	stream, ok, err := p.Adapter.BuildUpdate(ctx, req)
	if stream != nil {
		stream.Profile.AdapterName = p.Adapter.Name()
		stream.Profile.UsedAdapter = ok
	}
	return stream, ok, err
}

func (p Planner) tryAdapterMerge(ctx context.Context, req MergeRequest) (*ActionStream, bool, error) {
	if p.Adapter == nil || !p.Adapter.SupportsRowDelta() {
		return nil, false, nil
	}
	stream, ok, err := p.Adapter.BuildMerge(ctx, req)
	if stream != nil {
		stream.Profile.AdapterName = p.Adapter.Name()
		stream.Profile.UsedAdapter = ok
	}
	return stream, ok, err
}

type NativePlanner struct{}

func (NativePlanner) PlanDelete(ctx context.Context, req DeleteRequest) (*ActionStream, error) {
	base, err := validateBase(req.Base)
	if err != nil {
		return nil, err
	}
	stream := &ActionStream{Operation: OperationDelete, Base: base, Profile: Profile{Operation: OperationDelete}}
	for _, target := range req.Targets {
		action, err := deleteAction(target)
		if err != nil {
			return nil, err
		}
		stream.Actions = append(stream.Actions, action)
		stream.Profile.MatchedRows += target.MatchedRows
		addActionProfile(&stream.Profile, action)
	}
	if len(stream.Actions) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg DELETE requires at least one matched target", map[string]string{"table": req.Base.Table})
	}
	return stream, nil
}

func (NativePlanner) PlanUpdate(ctx context.Context, req UpdateRequest) (*ActionStream, error) {
	base, err := validateBase(req.Base)
	if err != nil {
		return nil, err
	}
	mode := req.Mode
	if mode == "" {
		mode = TableModeMergeOnRead
	}
	stream := &ActionStream{Operation: OperationUpdate, Base: base, Profile: Profile{Operation: OperationUpdate}}
	switch mode {
	case TableModeCopyOnWrite:
		for _, target := range req.Targets {
			replacements := firstNonEmptyFiles(target.ReplacementFiles, target.DeleteTarget.ReplacementFiles)
			if len(replacements) == 0 {
				return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg UPDATE copy-on-write requires replacement data files", map[string]string{"table": req.Base.Table})
			}
			action := Action{Kind: ActionRewriteDataFile, ReplacedFile: target.DataFile, ReplacementFiles: cloneDataFiles(replacements), Reason: "update-copy-on-write"}
			stream.Actions = append(stream.Actions, action)
			stream.Profile.MatchedRows += target.MatchedRows
			addActionProfile(&stream.Profile, action)
		}
	default:
		if !updateRequestHasReplacementFiles(req) {
			return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg UPDATE merge-on-read requires replacement data files", map[string]string{"table": req.Base.Table})
		}
		deleteReq := DeleteRequest{Base: base, Mode: TableModeMergeOnRead}
		for _, target := range req.Targets {
			deleteReq.Targets = append(deleteReq.Targets, target.DeleteTarget)
		}
		deleteStream, err := (NativePlanner{}).PlanDelete(ctx, deleteReq)
		if err != nil {
			return nil, err
		}
		stream.Actions = append(stream.Actions, deleteStream.Actions...)
		stream.Profile = mergeProfile(stream.Profile, deleteStream.Profile)
		for _, target := range req.Targets {
			for _, file := range firstNonEmptyFiles(target.ReplacementFiles, target.DeleteTarget.ReplacementFiles) {
				action := Action{Kind: ActionAppendData, File: file, Reason: "update-append-replacement"}
				stream.Actions = append(stream.Actions, action)
				addActionProfile(&stream.Profile, action)
			}
		}
		for _, file := range req.AppendedDataFile {
			action := Action{Kind: ActionAppendData, File: file, Reason: "update-append-replacement"}
			stream.Actions = append(stream.Actions, action)
			addActionProfile(&stream.Profile, action)
		}
	}
	if len(stream.Actions) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg UPDATE requires delete or replacement actions", map[string]string{"table": req.Base.Table})
	}
	return stream, nil
}

func updateRequestHasReplacementFiles(req UpdateRequest) bool {
	if len(req.AppendedDataFile) > 0 {
		return true
	}
	for _, target := range req.Targets {
		if len(target.ReplacementFiles) > 0 || len(target.DeleteTarget.ReplacementFiles) > 0 {
			return true
		}
	}
	return false
}

func (NativePlanner) PlanMerge(ctx context.Context, req MergeRequest) (*ActionStream, error) {
	base, err := validateBase(req.Base)
	if err != nil {
		return nil, err
	}
	stream := &ActionStream{Operation: OperationMerge, Base: base, Profile: Profile{Operation: OperationMerge}}
	if len(req.MatchedDeletes) > 0 {
		deleteStream, err := (NativePlanner{}).PlanDelete(ctx, DeleteRequest{Base: base, Mode: req.Mode, Targets: req.MatchedDeletes})
		if err != nil {
			return nil, err
		}
		stream.Actions = append(stream.Actions, deleteStream.Actions...)
		stream.Profile = mergeProfile(stream.Profile, deleteStream.Profile)
	}
	if len(req.MatchedUpdates) > 0 {
		updateStream, err := (NativePlanner{}).PlanUpdate(ctx, UpdateRequest{Base: base, Mode: req.Mode, Targets: req.MatchedUpdates})
		if err != nil {
			return nil, err
		}
		stream.Actions = append(stream.Actions, updateStream.Actions...)
		stream.Profile = mergeProfile(stream.Profile, updateStream.Profile)
	}
	for _, file := range req.UnmatchedAppends {
		action := Action{Kind: ActionAppendData, File: file, Reason: "merge-unmatched-append"}
		stream.Actions = append(stream.Actions, action)
		addActionProfile(&stream.Profile, action)
	}
	if len(stream.Actions) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg MERGE requires at least one action", map[string]string{"table": req.Base.Table})
	}
	return stream, nil
}

func (NativePlanner) PlanOverwrite(ctx context.Context, req OverwriteRequest) (*ActionStream, error) {
	base, err := validateBase(req.Base)
	if err != nil {
		return nil, err
	}
	if req.Scope == "" {
		req.Scope = OverwriteTable
	}
	stream := &ActionStream{Operation: OperationOverwrite, Base: base, Profile: Profile{Operation: OperationOverwrite}}
	for _, file := range req.AffectedDataFiles {
		action := Action{Kind: ActionDeleteDataFile, ReplacedFile: file, Reason: "overwrite-" + string(req.Scope)}
		stream.Actions = append(stream.Actions, action)
		addActionProfile(&stream.Profile, action)
	}
	for _, file := range req.ReplacementFiles {
		action := Action{Kind: ActionAppendData, File: file, Reason: "overwrite-replacement"}
		stream.Actions = append(stream.Actions, action)
		addActionProfile(&stream.Profile, action)
	}
	if len(stream.Actions) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg overwrite requires affected or replacement data files", map[string]string{"table": req.Base.Table})
	}
	return stream, nil
}

func BuildCommitIntent(stream ActionStream) (*CommitIntent, error) {
	base, err := validateBase(stream.Base)
	if err != nil {
		return nil, err
	}
	stream.Base = base
	if len(stream.Actions) == 0 {
		return nil, api.NewError(api.ErrMetadataInvalid, "Iceberg DML action stream is empty", map[string]string{"table": stream.Base.Table})
	}
	targetRef := strings.TrimSpace(stream.Base.TargetRef)
	if targetRef == "" {
		targetRef = "main"
	}
	requirements := []api.CommitRequirement{refSnapshotRequirement(targetRef, stream.Base.BaseSnapshotID)}
	if stream.Base.TableUUID != "" {
		requirements = append(requirements, api.CommitRequirement{Type: "assert-table-uuid", TableUUID: stream.Base.TableUUID})
	}
	requirements = append(requirements, api.CommitRequirement{Type: "assert-current-schema-id", SchemaID: stream.Base.BaseSchemaID})
	requirements = append(requirements, api.CommitRequirement{Type: "assert-default-spec-id", SpecID: stream.Base.BaseSpecID})
	profile := Profile{Operation: stream.Operation}
	for _, action := range stream.Actions {
		addActionProfile(&profile, action)
		switch action.Kind {
		case ActionAppendData, ActionAddEqualityDelete, ActionAddPositionDelete, ActionDeleteDataFile, ActionRewriteDataFile:
		default:
			return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg DML action kind is unsupported", map[string]string{"action": string(action.Kind)})
		}
	}
	summary := cloneStringMap(stream.Base.Summary)
	if summary == nil {
		summary = make(map[string]string)
	}
	summary["operation"] = string(stream.Operation)
	summary["engine"] = "matrixone"
	summary["idempotency-key"] = stream.Base.IdempotencyKey
	summary["added-data-files"] = strconv.Itoa(profile.AddedDataFiles)
	summary["added-delete-files"] = strconv.Itoa(profile.AddedDeleteFiles)
	summary["deleted-data-files"] = strconv.Itoa(profile.DeletedDataFiles)
	summary["rewritten-data-files"] = strconv.Itoa(profile.RewrittenDataFiles)
	return &CommitIntent{
		Requirements:   requirements,
		Actions:        cloneActions(stream.Actions),
		Summary:        summary,
		IdempotencyKey: stream.Base.IdempotencyKey,
		BaseSnapshotID: stream.Base.BaseSnapshotID,
		BaseSchemaID:   stream.Base.BaseSchemaID,
		BaseSpecID:     stream.Base.BaseSpecID,
		TargetRef:      targetRef,
		TargetRefType:  stream.Base.TargetRefType,
		Profile:        profile,
	}, nil
}

func BuildAuditProfile(stream ActionStream, intent *CommitIntent) map[string]string {
	out := map[string]string{
		"operation":             string(stream.Operation),
		"matched_rows":          strconv.FormatInt(stream.Profile.MatchedRows, 10),
		"added_data_files":      strconv.Itoa(stream.Profile.AddedDataFiles),
		"added_delete_files":    strconv.Itoa(stream.Profile.AddedDeleteFiles),
		"deleted_data_files":    strconv.Itoa(stream.Profile.DeletedDataFiles),
		"rewritten_data_files":  strconv.Itoa(stream.Profile.RewrittenDataFiles),
		"equality_delete_files": strconv.Itoa(stream.Profile.EqualityDeleteFiles),
		"position_delete_files": strconv.Itoa(stream.Profile.PositionDeleteFiles),
	}
	if intent != nil {
		out["idempotency_key"] = intent.IdempotencyKey
		out["target_ref"] = intent.TargetRef
	}
	if stream.Profile.AdapterName != "" {
		out["adapter"] = stream.Profile.AdapterName
		out["used_adapter"] = strconv.FormatBool(stream.Profile.UsedAdapter)
	}
	return out
}

func cloneActions(in []Action) []Action {
	if len(in) == 0 {
		return nil
	}
	out := make([]Action, len(in))
	copy(out, in)
	for i := range out {
		out[i].ReplacementFiles = cloneDataFiles(out[i].ReplacementFiles)
	}
	return out
}

func deleteAction(target DeleteTarget) (Action, error) {
	if len(target.EqualityFieldIDs) > 0 && target.PredicateStable && strings.TrimSpace(target.EqualityDeleteFile.FilePath) != "" {
		file := target.EqualityDeleteFile
		file.Content = api.DataFileContentEqualityDelete
		file.EqualityIDs = append([]int(nil), target.EqualityFieldIDs...)
		return Action{Kind: ActionAddEqualityDelete, DeleteFile: file, ReplacedFile: target.DataFile, Reason: "delete-equality"}, nil
	}
	if target.HasRowOrdinal && strings.TrimSpace(target.PositionDeleteFile.FilePath) != "" {
		file := target.PositionDeleteFile
		file.Content = api.DataFileContentPositionDelete
		if file.ReferencedDataFile == "" {
			file.ReferencedDataFile = target.DataFile.FilePath
		}
		return Action{Kind: ActionAddPositionDelete, DeleteFile: file, ReplacedFile: target.DataFile, Reason: "delete-position"}, nil
	}
	if len(target.ReplacementFiles) > 0 {
		return Action{Kind: ActionRewriteDataFile, ReplacedFile: target.DataFile, ReplacementFiles: cloneDataFiles(target.ReplacementFiles), Reason: "delete-copy-on-write"}, nil
	}
	return Action{}, api.NewError(api.ErrUnsupportedFeature, "Iceberg DELETE target cannot be represented as equality delete, position delete, or rewrite", map[string]string{
		"path": api.RedactPath(target.DataFile.FilePath),
	})
}

func validateBase(base CommitBase) (CommitBase, error) {
	if strings.TrimSpace(base.Table) == "" || len(base.Namespace) == 0 {
		return CommitBase{}, api.NewError(api.ErrConfigInvalid, "Iceberg DML requires namespace and table", nil)
	}
	if strings.TrimSpace(base.IdempotencyKey) == "" {
		return CommitBase{}, api.NewError(api.ErrConfigInvalid, "Iceberg DML requires an idempotency key", map[string]string{"table": base.Table})
	}
	return NormalizeCommitBaseRef(base)
}

func NormalizeCommitBaseRef(base CommitBase) (CommitBase, error) {
	raw := strings.TrimSpace(base.TargetRef)
	if raw == "" {
		raw = "main"
	}
	spec, err := icebergref.ParseNessieRef(raw, nil)
	if err != nil {
		return CommitBase{}, err
	}
	if refType := strings.ToLower(strings.TrimSpace(base.TargetRefType)); refType != "" {
		spec.Type = icebergref.Type(refType)
		if strings.TrimSpace(spec.Name) == "" {
			spec.Name = raw
		}
	}
	if err := icebergref.ValidateWrite(spec, base.CatalogCapabilities, base.AllowTagMove); err != nil {
		return CommitBase{}, err
	}
	base.TargetRef = spec.Name
	base.TargetRefType = string(spec.Type)
	return base, nil
}

func refSnapshotRequirement(ref string, snapshotID int64) api.CommitRequirement {
	if strings.TrimSpace(ref) == "" {
		ref = "main"
	}
	if snapshotID == 0 {
		return api.CommitRequirement{Type: "assert-ref-not-exists", Ref: ref}
	}
	return api.CommitRequirement{Type: "assert-ref-snapshot-id", Ref: ref, SnapshotID: snapshotID}
}

func addActionProfile(profile *Profile, action Action) {
	switch action.Kind {
	case ActionAppendData:
		profile.AddedDataFiles++
	case ActionAddEqualityDelete:
		profile.AddedDeleteFiles++
		profile.EqualityDeleteFiles++
	case ActionAddPositionDelete:
		profile.AddedDeleteFiles++
		profile.PositionDeleteFiles++
	case ActionDeleteDataFile:
		profile.DeletedDataFiles++
	case ActionRewriteDataFile:
		profile.RewrittenDataFiles++
		profile.DeletedDataFiles++
		profile.AddedDataFiles += len(action.ReplacementFiles)
		profile.CopyOnWriteFallbacks++
	}
}

func mergeProfile(left, right Profile) Profile {
	left.MatchedRows += right.MatchedRows
	left.AddedDataFiles += right.AddedDataFiles
	left.AddedDeleteFiles += right.AddedDeleteFiles
	left.DeletedDataFiles += right.DeletedDataFiles
	left.RewrittenDataFiles += right.RewrittenDataFiles
	left.EqualityDeleteFiles += right.EqualityDeleteFiles
	left.PositionDeleteFiles += right.PositionDeleteFiles
	left.CopyOnWriteFallbacks += right.CopyOnWriteFallbacks
	return left
}

func cloneDataFiles(in []api.DataFile) []api.DataFile {
	if len(in) == 0 {
		return nil
	}
	out := make([]api.DataFile, len(in))
	copy(out, in)
	for i := range out {
		out[i].PartitionFieldIDs = cloneStringIntMap(in[i].PartitionFieldIDs)
	}
	return out
}

func cloneStringIntMap(in map[string]int) map[string]int {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func firstNonEmptyFiles(a, b []api.DataFile) []api.DataFile {
	if len(a) > 0 {
		return a
	}
	return b
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
