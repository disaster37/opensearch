package api

import (
	"fmt"

	"github.com/disaster37/opensearch/v3/types"
	"github.com/go-playground/validator/v10"
)

var validate = validator.New(validator.WithRequiredStructEnabled())

func validationError(err error) error {
	if err != nil {
		return fmt.Errorf("validation: %w", err)
	}
	return nil
}

type IndexRequest struct {
	Index  string `validate:"required"`
	Id     string
	Body   any
	Params map[string]string
}

func (r *IndexRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type GetRequest struct {
	Index  string `validate:"required"`
	Id     string `validate:"required"`
	Params map[string]string
}

func (r *GetRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type DeleteRequest struct {
	Index  string `validate:"required"`
	Id     string `validate:"required"`
	Params map[string]string
}

func (r *DeleteRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type UpdateRequest struct {
	Index  string `validate:"required"`
	Id     string `validate:"required"`
	Body   any
	Params map[string]string
}

func (r *UpdateRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type SearchRequest struct {
	Indices []string
	Body    any
	Params  map[string]string
}

type ScrollRequest struct {
	ScrollId  string `validate:"required"`
	KeepAlive string
}

func (r *ScrollRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type FieldCapsRequest struct {
	Indices []string
	Fields  []string
}

type ShrinkRequest struct {
	Source string `validate:"required"`
	Target string `validate:"required"`
	Body   any
}

func (r *ShrinkRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type PutAliasRequest struct {
	Index string `validate:"required"`
	Alias string `validate:"required"`
	Body  any
}

func (r *PutAliasRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type GetFieldMappingRequest struct {
	Indices []string
	Fields  []string `validate:"required,min=1"`
}

func (r *GetFieldMappingRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type IndicesStatsRequest struct {
	Indices []string
	Metrics []string
}

type PutMappingRequest struct {
	Indices []string `validate:"required,min=1"`
	Body    any      `validate:"required"`
}

func (r *PutMappingRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type PutTemplateRequest struct {
	Name string `validate:"required"`
	Body any    `validate:"required"`
}

func (r *PutTemplateRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type PutIndexTemplateRequest struct {
	Name string `validate:"required"`
	Body any    `validate:"required"`
}

func (r *PutIndexTemplateRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type PutComponentTemplateRequest struct {
	Name string `validate:"required"`
	Body any    `validate:"required"`
}

func (r *PutComponentTemplateRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type SnapshotCreateRequest struct {
	Repository string `validate:"required"`
	Snapshot   string `validate:"required"`
	Body       any
}

func (r *SnapshotCreateRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type SnapshotGetRequest struct {
	Repository string `validate:"required"`
	Snapshots  []string
}

func (r *SnapshotGetRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type SnapshotDeleteRequest struct {
	Repository string `validate:"required"`
	Snapshot   string `validate:"required"`
}

func (r *SnapshotDeleteRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type SnapshotStatusRequest struct {
	Repository string `validate:"required"`
	Snapshots  []string
}

func (r *SnapshotStatusRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type SnapshotRestoreRequest struct {
	Repository string `validate:"required"`
	Snapshot   string `validate:"required"`
	Body       any
}

func (r *SnapshotRestoreRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type SnapshotCloneRequest struct {
	Repository     string `validate:"required"`
	Snapshot       string `validate:"required"`
	TargetSnapshot string `validate:"required"`
	Body           any
}

func (r *SnapshotCloneRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type SmPutPolicyRequest struct {
	PolicyName string       `validate:"required"`
	Body       *SmPutPolicy `validate:"required"`
	Version    *types.DocumentVersion
}

func (r *SmPutPolicyRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type IsmPutPolicyRequest struct {
	PolicyName string         `validate:"required"`
	Body       *IsmPolicyBase `validate:"required"`
	Version    *types.DocumentVersion
}

func (r *IsmPutPolicyRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type AlertingPutMonitorRequest struct {
	MonitorId string `validate:"required"`
	Body      any    `validate:"required"`
	Version   *types.DocumentVersion
}

func (r *AlertingPutMonitorRequest) Validate() error {
	return validationError(validate.Struct(r))
}

// AlertingIndexWorkflowRequest holds the parameters for creating or updating a workflow.
type AlertingIndexWorkflowRequest struct {
	WorkflowId string
	Body       any `validate:"required"`
	Version    *types.DocumentVersion
}

// Validate validates the AlertingIndexWorkflowRequest.
func (r *AlertingIndexWorkflowRequest) Validate() error {
	return validationError(validate.Struct(r))
}

// AdIndexDetectorRequest holds the parameters for creating or updating an anomaly detector.
type AdIndexDetectorRequest struct {
	DetectorId string
	Body       any `validate:"required"`
}

// Validate validates the AdIndexDetectorRequest.
func (r *AdIndexDetectorRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type TransformPutJobRequest struct {
	JobName string            `validate:"required"`
	Body    *TransformJobBase `validate:"required"`
	Version *types.DocumentVersion
}

func (r *TransformPutJobRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type ClusterStateRequest struct {
	Metrics []string
	Indices []string
}

type NodesInfoRequest struct {
	NodeIds []string
	Metrics []string
}

type NodesStatsRequest struct {
	NodeIds []string
	Metrics []string
}

type NodesUsageRequest struct {
	NodeIds []string
	Metrics []string
}

type CcrDeleteAutoFollowOptions struct {
	LeaderAlias string `validate:"required"`
	Name        string `validate:"required"`
}

func (r *CcrDeleteAutoFollowOptions) Validate() error {
	return validationError(validate.Struct(r))
}

type CcrStartRuleRequest struct {
	Name string `validate:"required"`
	Body *CcrRule
}

func (r *CcrStartRuleRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type IngestPutPipelineRequest struct {
	Id   string `validate:"required"`
	Body any    `validate:"required"`
}

func (r *IngestPutPipelineRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type IngestSimulatePipelineRequest struct {
	Id   string
	Body any `validate:"required"`
}

func (r *IngestSimulatePipelineRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type ScriptPutRequest struct {
	Id   string `validate:"required"`
	Body any    `validate:"required"`
}

func (r *ScriptPutRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type CreateRequest struct {
	Index  string `validate:"required"`
	Id     string `validate:"required"`
	Body   any    `validate:"required"`
	Params map[string]string
}

func (r *CreateRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type RethrottleRequest struct {
	TaskId            string `validate:"required"`
	RequestsPerSecond float64
}

func (r *RethrottleRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type SearchTemplateRequest struct {
	Indices []string
	Body    any `validate:"required"`
	Params  map[string]string
}

func (r *SearchTemplateRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type MultiSearchTemplateRequest struct {
	Indices []string
	Body    any `validate:"required"`
	Params  map[string]string
}

func (r *MultiSearchTemplateRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type RenderSearchTemplateRequest struct {
	Id   string
	Body any
}

type RankEvalRequest struct {
	Indices []string
	Body    any `validate:"required"`
	Params  map[string]string
}

func (r *RankEvalRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type CreatePITRequest struct {
	Indices   []string `validate:"required,min=1"`
	KeepAlive string   `validate:"required"`
	Params    map[string]string
}

func (r *CreatePITRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type DeletePITRequest struct {
	PitIds []string `validate:"required,min=1"`
}

func (r *DeletePITRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type CloneRequest struct {
	Source string `validate:"required"`
	Target string `validate:"required"`
	Body   any
}

func (r *CloneRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type SplitRequest struct {
	Source string `validate:"required"`
	Target string `validate:"required"`
	Body   any
}

func (r *SplitRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type AddBlockRequest struct {
	Indices []string `validate:"required,min=1"`
	Block   string   `validate:"required"`
}

func (r *AddBlockRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type DeleteAliasRequest struct {
	Indices []string `validate:"required,min=1"`
	Names   []string `validate:"required,min=1"`
}

func (r *DeleteAliasRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type SimulateIndexTemplateRequest struct {
	Name string `validate:"required"`
	Body any
}

func (r *SimulateIndexTemplateRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type SimulateTemplateRequest struct {
	Name string
	Body any
}
