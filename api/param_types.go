package api

// Refresh controls when changes become visible to search.
// Valid values: RefreshTrue, RefreshFalse, RefreshWaitFor
type Refresh string

const (
	RefreshTrue    Refresh = "true"
	RefreshFalse   Refresh = "false"
	RefreshWaitFor Refresh = "wait_for"
)

// VersionType controls versioning strategy for document operations.
// Valid values: VersionTypeInternal, VersionTypeExternal, VersionTypeExternalGte
type VersionType string

const (
	VersionTypeInternal    VersionType = "internal"
	VersionTypeExternal    VersionType = "external"
	VersionTypeExternalGte VersionType = "external_gte"
)

// SearchType controls how distributed term frequencies affect scoring.
type SearchType string

const (
	SearchTypeQueryThenFetch    SearchType = "query_then_fetch"
	SearchTypeDfsQueryThenFetch SearchType = "dfs_query_then_fetch"
)

// ExpandWildcard values. Can be combined (e.g. "open,closed").
const (
	ExpandWildcardOpen   = "open"
	ExpandWildcardClosed = "closed"
	ExpandWildcardNone   = "none"
	ExpandWildcardAll    = "all"
)

// DataStreamActionType identifies a modify action for POST /_data_stream/_modify
// (OpenSearch 3.8.0+).
type DataStreamActionType string

const (
	DataStreamActionAddBackingIndex    DataStreamActionType = "add_backing_index"
	DataStreamActionRemoveBackingIndex DataStreamActionType = "remove_backing_index"
)

// TierTarget selects the destination tier filter for TieringService.ListStatus
// (OpenSearch 3.7.0+).
type TierTarget string

const (
	TierTargetHot  TierTarget = "_hot"
	TierTargetWarm TierTarget = "_warm"
)

// IngestionResetMode selects how a consumer position is reset on resume
// (pull-based ingestion, GA in OpenSearch 3.6.0).
type IngestionResetMode string

const (
	IngestionResetModeOffset    IngestionResetMode = "OFFSET"
	IngestionResetModeTimestamp IngestionResetMode = "TIMESTAMP"
)
