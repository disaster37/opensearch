package api

import (
	"fmt"
	"strconv"

	json "github.com/goccy/go-json"

	"github.com/disaster37/opensearch/v4/querydsl"
	"github.com/disaster37/opensearch/v4/types"
	"github.com/go-playground/validator/v10"
)

// rawBody wraps a JSON string as json.RawMessage so that resty does not
// double-encode it when setting the request body.
func rawBody(s string) json.RawMessage {
	return json.RawMessage(s)
}

var validate = validator.New(validator.WithRequiredStructEnabled())

func validationError(err error) error {
	if err != nil {
		return fmt.Errorf("validation: %w", err)
	}
	return nil
}

type IndexParams struct {
	Refresh       string
	Routing       string
	Timeout       string
	Version       int64
	VersionType   string
	IfSeqNo       *int64
	IfPrimaryTerm *int64
	Pipeline      string
	RequireAlias  bool
}

func (p *IndexParams) ToMap() map[string]string {
	if p == nil {
		return nil
	}
	m := make(map[string]string)
	if p.Refresh != "" {
		m["refresh"] = p.Refresh
	}
	if p.Routing != "" {
		m["routing"] = p.Routing
	}
	if p.Timeout != "" {
		m["timeout"] = p.Timeout
	}
	if p.Version != 0 {
		m["version"] = strconv.FormatInt(p.Version, 10)
	}
	if p.VersionType != "" {
		m["version_type"] = p.VersionType
	}
	if p.IfSeqNo != nil {
		m["if_seq_no"] = strconv.FormatInt(*p.IfSeqNo, 10)
	}
	if p.IfPrimaryTerm != nil {
		m["if_primary_term"] = strconv.FormatInt(*p.IfPrimaryTerm, 10)
	}
	if p.Pipeline != "" {
		m["pipeline"] = p.Pipeline
	}
	if p.RequireAlias {
		m["require_alias"] = "true"
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

type GetParams struct {
	Routing        string
	Preference     string
	Realtime       bool
	Refresh        bool
	Source         string
	SourceExcludes string
	SourceIncludes string
	StoredFields   string
	Version        *int64
	VersionType    string
	IfSeqNo        *int64
	IfPrimaryTerm  *int64
}

func (p *GetParams) ToMap() map[string]string {
	if p == nil {
		return nil
	}
	m := make(map[string]string)
	if p.Routing != "" {
		m["routing"] = p.Routing
	}
	if p.Preference != "" {
		m["preference"] = p.Preference
	}
	if p.Realtime {
		m["realtime"] = "true"
	}
	if p.Refresh {
		m["refresh"] = "true"
	}
	if p.Source != "" {
		m["_source"] = p.Source
	}
	if p.SourceExcludes != "" {
		m["_source_excludes"] = p.SourceExcludes
	}
	if p.SourceIncludes != "" {
		m["_source_includes"] = p.SourceIncludes
	}
	if p.StoredFields != "" {
		m["stored_fields"] = p.StoredFields
	}
	if p.Version != nil {
		m["version"] = strconv.FormatInt(*p.Version, 10)
	}
	if p.VersionType != "" {
		m["version_type"] = p.VersionType
	}
	if p.IfSeqNo != nil {
		m["if_seq_no"] = strconv.FormatInt(*p.IfSeqNo, 10)
	}
	if p.IfPrimaryTerm != nil {
		m["if_primary_term"] = strconv.FormatInt(*p.IfPrimaryTerm, 10)
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

type DeleteParams struct {
	Refresh       string
	Routing       string
	Timeout       string
	Version       *int64
	VersionType   string
	IfSeqNo       *int64
	IfPrimaryTerm *int64
}

func (p *DeleteParams) ToMap() map[string]string {
	if p == nil {
		return nil
	}
	m := make(map[string]string)
	if p.Refresh != "" {
		m["refresh"] = p.Refresh
	}
	if p.Routing != "" {
		m["routing"] = p.Routing
	}
	if p.Timeout != "" {
		m["timeout"] = p.Timeout
	}
	if p.Version != nil {
		m["version"] = strconv.FormatInt(*p.Version, 10)
	}
	if p.VersionType != "" {
		m["version_type"] = p.VersionType
	}
	if p.IfSeqNo != nil {
		m["if_seq_no"] = strconv.FormatInt(*p.IfSeqNo, 10)
	}
	if p.IfPrimaryTerm != nil {
		m["if_primary_term"] = strconv.FormatInt(*p.IfPrimaryTerm, 10)
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

type UpdateParams struct {
	Refresh         string
	Routing         string
	Timeout         string
	RetryOnConflict *int
	Source          string
	SourceExcludes  string
	SourceIncludes  string
	IfSeqNo         *int64
	IfPrimaryTerm   *int64
	RequireAlias    bool
}

func (p *UpdateParams) ToMap() map[string]string {
	if p == nil {
		return nil
	}
	m := make(map[string]string)
	if p.Refresh != "" {
		m["refresh"] = p.Refresh
	}
	if p.Routing != "" {
		m["routing"] = p.Routing
	}
	if p.Timeout != "" {
		m["timeout"] = p.Timeout
	}
	if p.RetryOnConflict != nil {
		m["retry_on_conflict"] = strconv.Itoa(*p.RetryOnConflict)
	}
	if p.Source != "" {
		m["_source"] = p.Source
	}
	if p.SourceExcludes != "" {
		m["_source_excludes"] = p.SourceExcludes
	}
	if p.SourceIncludes != "" {
		m["_source_includes"] = p.SourceIncludes
	}
	if p.IfSeqNo != nil {
		m["if_seq_no"] = strconv.FormatInt(*p.IfSeqNo, 10)
	}
	if p.IfPrimaryTerm != nil {
		m["if_primary_term"] = strconv.FormatInt(*p.IfPrimaryTerm, 10)
	}
	if p.RequireAlias {
		m["require_alias"] = "true"
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

type SearchParams struct {
	SearchType                 string
	Routing                    string
	Preference                 string
	RequestCache               bool
	Scroll                     string
	AllowPartialSearchResults  bool
	BatchedReduceSize          *int
	MaxConcurrentShardRequests *int
	PreFilterShardSize         *int
	RestTotalHitsAsInt         bool
	TypedKeys                  bool
	IgnoreUnavailable          bool
	AllowNoIndices             bool
	ExpandWildcards            string
	CcsMinimizeRoundtrips      bool
	Explain                    bool
	Size                       *int
	From                       *int
	Timeout                    string
	TrackTotalHits             string
}

func (p *SearchParams) ToMap() map[string]string {
	if p == nil {
		return nil
	}
	m := make(map[string]string)
	if p.SearchType != "" {
		m["search_type"] = p.SearchType
	}
	if p.Routing != "" {
		m["routing"] = p.Routing
	}
	if p.Preference != "" {
		m["preference"] = p.Preference
	}
	if p.RequestCache {
		m["request_cache"] = "true"
	}
	if p.Scroll != "" {
		m["scroll"] = p.Scroll
	}
	if p.AllowPartialSearchResults {
		m["allow_partial_search_results"] = "true"
	}
	if p.BatchedReduceSize != nil {
		m["batched_reduce_size"] = strconv.Itoa(*p.BatchedReduceSize)
	}
	if p.MaxConcurrentShardRequests != nil {
		m["max_concurrent_shard_requests"] = strconv.Itoa(*p.MaxConcurrentShardRequests)
	}
	if p.PreFilterShardSize != nil {
		m["pre_filter_shard_size"] = strconv.Itoa(*p.PreFilterShardSize)
	}
	if p.RestTotalHitsAsInt {
		m["rest_total_hits_as_int"] = "true"
	}
	if p.TypedKeys {
		m["typed_keys"] = "true"
	}
	if p.IgnoreUnavailable {
		m["ignore_unavailable"] = "true"
	}
	if p.AllowNoIndices {
		m["allow_no_indices"] = "true"
	}
	if p.ExpandWildcards != "" {
		m["expand_wildcards"] = p.ExpandWildcards
	}
	if p.CcsMinimizeRoundtrips {
		m["ccs_minimize_roundtrips"] = "true"
	}
	if p.Explain {
		m["explain"] = "true"
	}
	if p.Size != nil {
		m["size"] = strconv.Itoa(*p.Size)
	}
	if p.From != nil {
		m["from"] = strconv.Itoa(*p.From)
	}
	if p.Timeout != "" {
		m["timeout"] = p.Timeout
	}
	if p.TrackTotalHits != "" {
		m["track_total_hits"] = p.TrackTotalHits
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

type SearchTemplateParams struct {
	SearchType            string
	Routing               string
	Preference            string
	RequestCache          bool
	Scroll                string
	IgnoreUnavailable     bool
	AllowNoIndices        bool
	ExpandWildcards       string
	Explain               bool
	Profile               bool
	TypedKeys             bool
	RestTotalHitsAsInt    bool
	CcsMinimizeRoundtrips bool
}

func (p *SearchTemplateParams) ToMap() map[string]string {
	if p == nil {
		return nil
	}
	m := make(map[string]string)
	if p.SearchType != "" {
		m["search_type"] = p.SearchType
	}
	if p.Routing != "" {
		m["routing"] = p.Routing
	}
	if p.Preference != "" {
		m["preference"] = p.Preference
	}
	if p.RequestCache {
		m["request_cache"] = "true"
	}
	if p.Scroll != "" {
		m["scroll"] = p.Scroll
	}
	if p.IgnoreUnavailable {
		m["ignore_unavailable"] = "true"
	}
	if p.AllowNoIndices {
		m["allow_no_indices"] = "true"
	}
	if p.ExpandWildcards != "" {
		m["expand_wildcards"] = p.ExpandWildcards
	}
	if p.Explain {
		m["explain"] = "true"
	}
	if p.Profile {
		m["profile"] = "true"
	}
	if p.TypedKeys {
		m["typed_keys"] = "true"
	}
	if p.RestTotalHitsAsInt {
		m["rest_total_hits_as_int"] = "true"
	}
	if p.CcsMinimizeRoundtrips {
		m["ccs_minimize_roundtrips"] = "true"
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

type MultiSearchTemplateParams struct {
	SearchType                 string
	MaxConcurrentSearches      *int
	TypedKeys                  bool
	RestTotalHitsAsInt         bool
	PreFilterShardSize         *int
	MaxConcurrentShardRequests *int
	CcsMinimizeRoundtrips      bool
}

func (p *MultiSearchTemplateParams) ToMap() map[string]string {
	if p == nil {
		return nil
	}
	m := make(map[string]string)
	if p.SearchType != "" {
		m["search_type"] = p.SearchType
	}
	if p.MaxConcurrentSearches != nil {
		m["max_concurrent_searches"] = strconv.Itoa(*p.MaxConcurrentSearches)
	}
	if p.TypedKeys {
		m["typed_keys"] = "true"
	}
	if p.RestTotalHitsAsInt {
		m["rest_total_hits_as_int"] = "true"
	}
	if p.PreFilterShardSize != nil {
		m["pre_filter_shard_size"] = strconv.Itoa(*p.PreFilterShardSize)
	}
	if p.MaxConcurrentShardRequests != nil {
		m["max_concurrent_shard_requests"] = strconv.Itoa(*p.MaxConcurrentShardRequests)
	}
	if p.CcsMinimizeRoundtrips {
		m["ccs_minimize_roundtrips"] = "true"
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

type RankEvalParams struct {
	SearchType        string
	Routing           string
	Preference        string
	RequestCache      bool
	IgnoreUnavailable bool
	AllowNoIndices    bool
	ExpandWildcards   string
}

func (p *RankEvalParams) ToMap() map[string]string {
	if p == nil {
		return nil
	}
	m := make(map[string]string)
	if p.SearchType != "" {
		m["search_type"] = p.SearchType
	}
	if p.Routing != "" {
		m["routing"] = p.Routing
	}
	if p.Preference != "" {
		m["preference"] = p.Preference
	}
	if p.RequestCache {
		m["request_cache"] = "true"
	}
	if p.IgnoreUnavailable {
		m["ignore_unavailable"] = "true"
	}
	if p.AllowNoIndices {
		m["allow_no_indices"] = "true"
	}
	if p.ExpandWildcards != "" {
		m["expand_wildcards"] = p.ExpandWildcards
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

type CreatePITParams struct {
	Routing                 string
	Preference              string
	IgnoreUnavailable       bool
	AllowNoIndices          bool
	ExpandWildcards         string
	AllowPartialPitCreation bool
}

func (p *CreatePITParams) ToMap() map[string]string {
	if p == nil {
		return nil
	}
	m := make(map[string]string)
	if p.Routing != "" {
		m["routing"] = p.Routing
	}
	if p.Preference != "" {
		m["preference"] = p.Preference
	}
	if p.IgnoreUnavailable {
		m["ignore_unavailable"] = "true"
	}
	if p.AllowNoIndices {
		m["allow_no_indices"] = "true"
	}
	if p.ExpandWildcards != "" {
		m["expand_wildcards"] = p.ExpandWildcards
	}
	if p.AllowPartialPitCreation {
		m["allow_partial_pit_creation"] = "true"
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

type IndexRequest struct {
	Index  string `validate:"required"`
	Id     string
	Body   any
	Params *IndexParams
}

func (r *IndexRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type GetRequest struct {
	Index  string `validate:"required"`
	Id     string `validate:"required"`
	Params *GetParams
}

func (r *GetRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type DeleteRequest struct {
	Index  string `validate:"required"`
	Id     string `validate:"required"`
	Params *DeleteParams
}

func (r *DeleteRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type UpdateRequest struct {
	Index  string `validate:"required"`
	Id     string `validate:"required"`
	Body   any
	Params *UpdateParams
}

func (r *UpdateRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type SearchRequest struct {
	Indices []string
	Body    any
	Params  *SearchParams
}

// NewSearchRequest creates a SearchRequest from a *querydsl.SearchRequest.
// It calls Body() on the querydsl request to serialize the query DSL and
// uses the result as the request body. This allows building search requests
// using the fluent querydsl builder API:
//
//	req, err := api.NewSearchRequest(
//	    querydsl.NewSearchRequest().
//	        Index("my-index").
//	        Query(querydsl.MatchAll{}).
//	        Size(10),
//	)
//	if err != nil {
//	    return err
//	}
//	result, err := client.Search().Search(ctx, req)
func NewSearchRequest(r *querydsl.SearchRequest) (*SearchRequest, error) {
	body, err := r.Body()
	if err != nil {
		return nil, fmt.Errorf("search request body: %w", err)
	}
	params := searchParamsFromURLParams(r.URLParams())
	return &SearchRequest{
		Indices: r.Indices(),
		Body:    rawBody(body),
		Params:  params,
	}, nil
}

func searchParamsFromURLParams(urlParams map[string]string) *SearchParams {
	if len(urlParams) == 0 {
		return nil
	}
	p := &SearchParams{}
	if v, ok := urlParams["search_type"]; ok {
		p.SearchType = v
	}
	if v, ok := urlParams["routing"]; ok {
		p.Routing = v
	}
	if v, ok := urlParams["preference"]; ok {
		p.Preference = v
	}
	if v, ok := urlParams["request_cache"]; ok {
		p.RequestCache = v == "true"
	}
	if v, ok := urlParams["scroll"]; ok {
		p.Scroll = v
	}
	if v, ok := urlParams["allow_partial_search_results"]; ok {
		p.AllowPartialSearchResults = v == "true"
	}
	if v, ok := urlParams["batched_reduce_size"]; ok {
		n, _ := strconv.Atoi(v)
		p.BatchedReduceSize = &n
	}
	if v, ok := urlParams["max_concurrent_shard_requests"]; ok {
		n, _ := strconv.Atoi(v)
		p.MaxConcurrentShardRequests = &n
	}
	if v, ok := urlParams["pre_filter_shard_size"]; ok {
		n, _ := strconv.Atoi(v)
		p.PreFilterShardSize = &n
	}
	if v, ok := urlParams["rest_total_hits_as_int"]; ok {
		p.RestTotalHitsAsInt = v == "true"
	}
	if v, ok := urlParams["typed_keys"]; ok {
		p.TypedKeys = v == "true"
	}
	if v, ok := urlParams["ignore_unavailable"]; ok {
		p.IgnoreUnavailable = v == "true"
	}
	if v, ok := urlParams["allow_no_indices"]; ok {
		p.AllowNoIndices = v == "true"
	}
	if v, ok := urlParams["expand_wildcards"]; ok {
		p.ExpandWildcards = v
	}
	if v, ok := urlParams["ccs_minimize_roundtrips"]; ok {
		p.CcsMinimizeRoundtrips = v == "true"
	}
	if v, ok := urlParams["explain"]; ok {
		p.Explain = v == "true"
	}
	if v, ok := urlParams["size"]; ok {
		n, _ := strconv.Atoi(v)
		p.Size = &n
	}
	if v, ok := urlParams["from"]; ok {
		n, _ := strconv.Atoi(v)
		p.From = &n
	}
	if v, ok := urlParams["timeout"]; ok {
		p.Timeout = v
	}
	if v, ok := urlParams["track_total_hits"]; ok {
		p.TrackTotalHits = v
	}
	return p
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
	Params *IndexParams
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
	Params  *SearchTemplateParams
}

func (r *SearchTemplateRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type MultiSearchTemplateRequest struct {
	Indices []string
	Body    any `validate:"required"`
	Params  *MultiSearchTemplateParams
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
	Params  *RankEvalParams
}

func (r *RankEvalRequest) Validate() error {
	return validationError(validate.Struct(r))
}

type CreatePITRequest struct {
	Indices   []string `validate:"required,min=1"`
	KeepAlive string   `validate:"required"`
	Params    *CreatePITParams
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
