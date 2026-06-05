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
	VersionTypeInternal     VersionType = "internal"
	VersionTypeExternal     VersionType = "external"
	VersionTypeExternalGte  VersionType = "external_gte"
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