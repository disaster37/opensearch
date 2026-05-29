package api

import (
	json "github.com/goccy/go-json"

	"github.com/disaster37/opensearch/v3/types"
)

// ScriptGetResponse represents the result of retrieving a stored script.
// The Id is the script identifier, Found indicates whether it exists, and
// Script contains the raw JSON script definition (lang + source).
type ScriptGetResponse struct {
	Id     string          `json:"_id"`
	Found  bool            `json:"found"`
	Script json.RawMessage `json:"script"`
}

// ScriptPutResponse represents the acknowledgment response from creating
// or updating a stored script. It embeds types.AcknowledgedResponse which
// contains the Acknowledged boolean.
type ScriptPutResponse struct {
	types.AcknowledgedResponse
}

// ScriptDeleteResponse represents the acknowledgment response from deleting
// a stored script. It embeds types.AcknowledgedResponse which contains
// the Acknowledged boolean.
type ScriptDeleteResponse struct {
	types.AcknowledgedResponse
}

// PainlessExecuteResponse represents the result of executing a Painless script
// via the scripts_painless_execute API. The Result field contains the raw
// JSON result produced by the script.
type PainlessExecuteResponse struct {
	Result json.RawMessage `json:"result"`
}

// ScriptContextResponse represents the result of the get script context API.
// Contexts lists the available script contexts and their allowed methods/parameters.
type ScriptContextResponse struct {
	Contexts []ScriptContext `json:"contexts"`
}

// ScriptContext describes a single script context (e.g. "ingest", "score").
type ScriptContext struct {
	Name    string           `json:"name"`
	Methods []map[string]any `json:"methods,omitempty"`
}

// ScriptLanguagesResponse represents the result of the get script language API.
// LanguageContexts lists the available script languages and their allowed contexts.
type ScriptLanguagesResponse struct {
	LanguageContexts []ScriptLanguageContext `json:"language_contexts"`
	TypesAllowed     []string                `json:"types_allowed,omitempty"`
}

// ScriptLanguageContext describes a script language and the contexts in which it is allowed.
type ScriptLanguageContext struct {
	Language string   `json:"language"`
	Contexts []string `json:"contexts"`
}
