package querydsl

import (
	json "github.com/goccy/go-json"
	"net/http"
	"time"

	"github.com/disaster37/opensearch/v3/types"
)

type SearchResult struct {
	Header          http.Header                   `json:"-"`
	TookInMillis    int64                         `json:"took,omitempty"`
	TerminatedEarly bool                          `json:"terminated_early,omitempty"`
	NumReducePhases int                           `json:"num_reduce_phases,omitempty"`
	Clusters        *SearchResultCluster          `json:"_clusters,omitempty"`
	ScrollId        string                        `json:"_scroll_id,omitempty"`
	Hits            *SearchHits                   `json:"hits,omitempty"`
	Suggest         SearchSuggest                 `json:"suggest,omitempty"`
	Aggregations    Aggregations                  `json:"aggregations,omitempty"`
	TimedOut        bool                          `json:"timed_out,omitempty"`
	Error           *types.OpenSearchErrorDetails `json:"error,omitempty"`
	Profile         *SearchProfile                `json:"profile,omitempty"`
	Shards          *types.ShardsInfo             `json:"_shards,omitempty"`
	Status          int                           `json:"status,omitempty"`
	PitId           string                        `json:"pit_id,omitempty"`
}

type SearchResultCluster struct {
	Successful int `json:"successful,omitempty"`
	Total      int `json:"total,omitempty"`
	Skipped    int `json:"skipped,omitempty"`
}

type SearchHits struct {
	TotalHits *TotalHits   `json:"total,omitempty"`
	MaxScore  *float64     `json:"max_score,omitempty"`
	Hits      []*SearchHit `json:"hits,omitempty"`
}

type NestedHit struct {
	Field  string     `json:"field"`
	Offset int        `json:"offset,omitempty"`
	Child  *NestedHit `json:"_nested,omitempty"`
}

type TotalHits struct {
	Value    int64  `json:"value"`
	Relation string `json:"relation"`
}

type SearchHit struct {
	Score          *float64                       `json:"_score,omitempty"`
	Index          string                         `json:"_index,omitempty"`
	Type           string                         `json:"_type,omitempty"`
	Id             string                         `json:"_id,omitempty"`
	Uid            string                         `json:"_uid,omitempty"`
	Routing        string                         `json:"_routing,omitempty"`
	Parent         string                         `json:"_parent,omitempty"`
	Version        *int64                         `json:"_version,omitempty"`
	SeqNo          *int64                         `json:"_seq_no"`
	PrimaryTerm    *int64                         `json:"_primary_term"`
	Sort           []any                          `json:"sort,omitempty"`
	Highlight      SearchHitHighlight             `json:"highlight,omitempty"`
	Source         json.RawMessage                `json:"_source,omitempty"`
	Fields         SearchHitFields                `json:"fields,omitempty"`
	Explanation    *SearchExplanation             `json:"_explanation,omitempty"`
	MatchedQueries []string                       `json:"matched_queries,omitempty"`
	InnerHits      map[string]*SearchHitInnerHits `json:"inner_hits,omitempty"`
	Nested         *NestedHit                     `json:"_nested,omitempty"`
	Shard          string                         `json:"_shard,omitempty"`
	Node           string                         `json:"_node,omitempty"`
}

type SearchHitFields map[string]any

type SearchHitInnerHits struct {
	Hits *SearchHits `json:"hits,omitempty"`
}

type SearchExplanation struct {
	Value       float64             `json:"value"`
	Description string              `json:"description"`
	Details     []SearchExplanation `json:"details,omitempty"`
}

type SearchSuggest map[string][]SearchSuggestion

type SearchSuggestion struct {
	Text    string                   `json:"text"`
	Offset  int                      `json:"offset"`
	Length  int                      `json:"length"`
	Options []SearchSuggestionOption `json:"options"`
}

type SearchSuggestionOption struct {
	Text            string              `json:"text"`
	Index           string              `json:"_index"`
	Type            string              `json:"_type"`
	Id              string              `json:"_id"`
	Score           float64             `json:"score"`
	ScoreUnderscore float64             `json:"_score"`
	Highlighted     string              `json:"highlighted"`
	CollateMatch    bool                `json:"collate_match"`
	Freq            int                 `json:"freq"`
	Source          json.RawMessage     `json:"_source"`
	Contexts        map[string][]string `json:"contexts,omitempty"`
}

type SearchProfile struct {
	Shards []SearchProfileShardResult `json:"shards"`
}

type SearchProfileShardResult struct {
	ID           string                    `json:"id"`
	Searches     []QueryProfileShardResult `json:"searches"`
	Aggregations []ProfileResult           `json:"aggregations"`
	Fetch        []ProfileResult           `json:"fetch"`
}

type QueryProfileShardResult struct {
	Query       []ProfileResult `json:"query,omitempty"`
	RewriteTime int64           `json:"rewrite_time,omitempty"`
	Collector   []any           `json:"collector,omitempty"`
}

type CollectorResult struct {
	Name      string            `json:"name,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	Time      string            `json:"time,omitempty"`
	TimeNanos int64             `json:"time_in_nanos,omitempty"`
	Children  []CollectorResult `json:"children,omitempty"`
}

type ProfileResult struct {
	Type          string           `json:"type"`
	Description   string           `json:"description,omitempty"`
	NodeTime      string           `json:"time,omitempty"`
	NodeTimeNanos int64            `json:"time_in_nanos,omitempty"`
	Breakdown     map[string]int64 `json:"breakdown,omitempty"`
	Children      []ProfileResult  `json:"children,omitempty"`
	Debug         map[string]any   `json:"debug,omitempty"`
}

type SearchHitHighlight map[string][]string

type MultiSearchResult struct {
	TookInMillis int64           `json:"took,omitempty"`
	Responses    []*SearchResult `json:"responses,omitempty"`
}

type CountResponse struct {
	Count           int64             `json:"count"`
	TerminatedEarly bool              `json:"terminated_early,omitempty"`
	Shards          *types.ShardsInfo `json:"_shards,omitempty"`
}

type ClearScrollResponse struct {
	Succeeded bool `json:"succeeded,omitempty"`
	NumFreed  int  `json:"num_freed,omitempty"`
}

type SearchShardsResponse struct {
	Nodes   map[string]any                      `json:"nodes"`
	Indices map[string]any                      `json:"indices"`
	Shards  [][]*SearchShardsResponseShardsInfo `json:"shards"`
}

type SearchShardsResponseShardsInfo struct {
	Index                    string          `json:"index"`
	Node                     string          `json:"node"`
	Primary                  bool            `json:"primary"`
	Shard                    uint            `json:"shard"`
	State                    string          `json:"state"`
	AllocationId             *AllocationId   `json:"allocation_id,omitempty"`
	RelocatingNode           string          `json:"relocating_node"`
	ExpectedShardSizeInBytes int64           `json:"expected_shard_size_in_bytes,omitempty"`
	RecoverySource           *RecoverySource `json:"recovery_source,omitempty"`
	UnassignedInfo           *UnassignedInfo `json:"unassigned_info,omitempty"`
}

type RecoverySource struct {
	Type string `json:"type"`
}

type AllocationId struct {
	Id           string `json:"id"`
	RelocationId string `json:"relocation_id,omitempty"`
}

type UnassignedInfo struct {
	Reason           string     `json:"reason"`
	At               *time.Time `json:"at,omitempty"`
	FailedAttempts   int        `json:"failed_attempts,omitempty"`
	Delayed          bool       `json:"delayed"`
	Details          string     `json:"details,omitempty"`
	AllocationStatus string     `json:"allocation_status"`
}

type ValidateResponse struct {
	Valid        bool           `json:"valid"`
	Shards       map[string]any `json:"_shards"`
	Explanations []any          `json:"explanations"`
}

type FieldCapsRequest struct {
	Fields      []string `json:"fields"`
	IndexFilter Query    `json:"index_filter,omitempty"`
}

type FieldCapsResponse struct {
	Indices []string                 `json:"indices,omitempty"`
	Fields  map[string]FieldCapsType `json:"fields,omitempty"`
}

type FieldCapsType map[string]FieldCaps

type FieldCaps struct {
	Type                   string         `json:"type"`
	MetadataField          bool           `json:"metadata_field"`
	Searchable             bool           `json:"searchable"`
	Aggregatable           bool           `json:"aggregatable"`
	Indices                []string       `json:"indices,omitempty"`
	NonSearchableIndices   []string       `json:"non_searchable_indices,omitempty"`
	NonAggregatableIndices []string       `json:"non_aggregatable_indices,omitempty"`
	Meta                   map[string]any `json:"meta,omitempty"`
}

type CreatePITResponse struct {
	PitId        string            `json:"pit_id"`
	CreationTime int64             `json:"creation_time"`
	KeepAlive    string            `json:"keep_alive,omitempty"`
	Shards       *types.ShardsInfo `json:"_shards,omitempty"`
}

type ListPITResponse struct {
	Pits []*PITInfo `json:"pits"`
}

type PITInfo struct {
	PitId        string `json:"pit_id"`
	CreationTime int64  `json:"creation_time"`
	KeepAlive    int64  `json:"keep_alive,omitempty"`
}

type DeletePITResponse struct {
	Pits []*DeletedPIT `json:"pits"`
}

type DeletedPIT struct {
	PitId      string `json:"pit_id"`
	Successful bool   `json:"successful"`
}

type RenderSearchTemplateResponse struct {
	TemplateOutput json.RawMessage `json:"template_output"`
}

type RankEvalResponse struct {
	RankEvalScore float64                    `json:"rank_eval_score"`
	Details       map[string]*RankEvalDetail `json:"details,omitempty"`
	Failures      map[string]string          `json:"failures,omitempty"`
}

type RankEvalDetail struct {
	Hits        []*RankEvalHit        `json:"hits,omitempty"`
	MetricScore float64               `json:"metric_score"`
	UnratedDocs []*RankEvalUnratedDoc `json:"unrated_docs,omitempty"`
}

type RankEvalHit struct {
	Index  string  `json:"_index"`
	Id     string  `json:"_id"`
	Score  float64 `json:"_score,omitempty"`
	Rating *int    `json:"rating,omitempty"`
}

type RankEvalUnratedDoc struct {
	Index string `json:"_index"`
	Id    string `json:"_id"`
}
