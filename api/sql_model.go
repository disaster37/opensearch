package api

// SQLResponse represents the standard response from the SQL/PPL plugin.
type SQLResponse struct {
	Schema     []map[string]any `json:"schema"`
	Datapoints [][]any          `json:"datarows"`
	Total      int64            `json:"total"`
	Size       int64            `json:"size"`
	Status     int64            `json:"status"`
	Cursor     *string          `json:"cursor,omitempty"`
}

// SQLExplainResponse represents the response from the SQL/PPL explain endpoint.
type SQLExplainResponse struct {
	Root map[string]any `json:"root"`
}

// SQLCloseResponse represents the response from closing an SQL cursor.
type SQLCloseResponse struct {
	Success bool `json:"success"`
}
