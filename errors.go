// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"emperror.dev/errors"
)

// checkResponse will return an error if the request/response indicates
// an error returned from Opensearch.
//
// HTTP status codes between in the range [200..299] are considered successful.
// All other errors are considered errors except they are specified in
// ignoreErrors. This is necessary because for some services, HTTP status 404
// is a valid response from Opensearch (e.g. the Exists service).
//
// The func tries to parse error details as returned from Opensearch
// and encapsulates them in type opensearch.Error.
func checkResponse(req *http.Request, res *http.Response, ignoreErrors ...int) error {
	// 200-299 are valid status codes
	if res.StatusCode >= 200 && res.StatusCode <= 299 {
		return nil
	}
	// Ignore certain errors?
	for _, code := range ignoreErrors {
		if code == res.StatusCode {
			return nil
		}
	}
	return createResponseError(res)
}

// createResponseError creates an Error structure from the HTTP response,
// its status code and the error information sent by Opensearch.
func createResponseError(res *http.Response) error {
	if res.Body == nil {
		return &Error{Status: res.StatusCode}
	}
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return &Error{Status: res.StatusCode}
	}
	errReply := new(Error)
	err = json.Unmarshal(data, errReply)
	if err != nil {
		return &Error{Status: res.StatusCode}
	}

	if errReply.Status == 0 {
		errReply.Status = res.StatusCode
	}
	return errReply
}

// Error encapsulates error details as returned from Opensearch.
type Error struct {
	Status  int           `json:"status"`
	Details *ErrorDetails `json:"error,omitempty"`
}

// ErrorDetails encapsulate error details from Opensearch.
// It is used in e.g. opensearch.Error and opensearch.BulkResponseItem.
type ErrorDetails struct {
	Type         string                   `json:"type"`
	Reason       string                   `json:"reason"`
	ResourceType string                   `json:"resource.type,omitempty"`
	ResourceId   string                   `json:"resource.id,omitempty"`
	Index        string                   `json:"index,omitempty"`
	Phase        string                   `json:"phase,omitempty"`
	Grouped      bool                     `json:"grouped,omitempty"`
	CausedBy     map[string]interface{}   `json:"caused_by,omitempty"`
	RootCause    []*ErrorDetails          `json:"root_cause,omitempty"`
	Suppressed   []*ErrorDetails          `json:"suppressed,omitempty"`
	FailedShards []map[string]interface{} `json:"failed_shards,omitempty"`
	Header       map[string]interface{}   `json:"header,omitempty"`

	// ScriptException adds the information in the following block.

	ScriptStack []string             `json:"script_stack,omitempty"` // from ScriptException
	Script      string               `json:"script,omitempty"`       // from ScriptException
	Lang        string               `json:"lang,omitempty"`         // from ScriptException
	Position    *ScriptErrorPosition `json:"position,omitempty"`     // from ScriptException (7.7+)
}

// ScriptErrorPosition specifies the position of the error
// in a script. It is used in ErrorDetails for scripting errors.
type ScriptErrorPosition struct {
	Offset int `json:"offset"`
	Start  int `json:"start"`
	End    int `json:"end"`
}

// Error returns a string representation of the error.
func (e *Error) Error() string {
	if e.Details != nil && e.Details.Reason != "" {
		return fmt.Sprintf("opensearch: Error %d (%s): %s [type=%s]", e.Status, http.StatusText(e.Status), e.Details.Reason, e.Details.Type)
	}
	return fmt.Sprintf("opensearch: Error %d (%s)", e.Status, http.StatusText(e.Status))
}

// ErrorReason returns the reason of an error that Opensearch reported,
// if err is of kind Error and has ErrorDetails with a Reason. Any other
// value of err will return an empty string.
func ErrorReason(err error) string {
	if err == nil {
		return ""
	}
	e, ok := err.(*Error)
	if !ok || e == nil || e.Details == nil {
		return ""
	}
	return e.Details.Reason
}

// IsContextErr returns true if the error is from a context that was canceled or deadline exceeded
func IsContextErr(err error) bool {
	if err == context.Canceled || err == context.DeadlineExceeded {
		return true
	}
	// This happens e.g. on redirect errors, see https://golang.org/src/net/http/client_test.go#L329
	if ue, ok := err.(*url.Error); ok {
		if ue.Temporary() {
			return true
		}
		// Use of an AWS Signing Transport can result in a wrapped url.Error
		return IsContextErr(ue.Err)
	}
	return false
}

// IsConnErr returns true if the error indicates that Elastic could not
// find an Opensearch host to connect to.
func IsConnErr(err error) bool {
	return err == ErrNoClient || errors.Is(err, ErrNoClient)
}

// IsNotFound returns true if the given error indicates that Opensearch
// returned HTTP status 404. The err parameter can be of type *opensearch.Error,
// opensearch.Error, *http.Response or int (indicating the HTTP status code).
func IsNotFound(err interface{}) bool {
	return IsStatusCode(err, http.StatusNotFound)
}

// IsTimeout returns true if the given error indicates that Opensearch
// returned HTTP status 408. The err parameter can be of type *opensearch.Error,
// opensearch.Error, *http.Response or int (indicating the HTTP status code).
func IsTimeout(err interface{}) bool {
	return IsStatusCode(err, http.StatusRequestTimeout)
}

// IsConflict returns true if the given error indicates that the Opensearch
// operation resulted in a version conflict. This can occur in operations like
// `update` or `index` with `op_type=create`. The err parameter can be of
// type *opensearch.Error, opensearch.Error, *http.Response or int (indicating the
// HTTP status code).
func IsConflict(err interface{}) bool {
	return IsStatusCode(err, http.StatusConflict)
}

// IsUnauthorized returns true if the given error indicates that
// Opensearch returned HTTP status 401. This happens e.g. when the
// cluster is configured to require HTTP Basic Auth.
// The err parameter can be of type *opensearch.Error, opensearch.Error,
// *http.Response or int (indicating the HTTP status code).
func IsUnauthorized(err interface{}) bool {
	return IsStatusCode(err, http.StatusUnauthorized)
}

// IsForbidden returns true if the given error indicates that Opensearch
// returned HTTP status 403. This happens e.g. due to a missing license.
// The err parameter can be of type *opensearch.Error, opensearch.Error,
// *http.Response or int (indicating the HTTP status code).
func IsForbidden(err interface{}) bool {
	return IsStatusCode(err, http.StatusForbidden)
}

// IsStatusCode returns true if the given error indicates that the Opensearch
// operation returned the specified HTTP status code. The err parameter can be of
// type *http.Response, *Error, Error, or int (indicating the HTTP status code).
func IsStatusCode(err interface{}, code int) bool {
	switch e := err.(type) {
	case *http.Response:
		return e.StatusCode == code
	case *Error:
		return e.Status == code
	case Error:
		return e.Status == code
	case int:
		return e == code
	}
	return false
}

// -- General errors --

// ShardsInfo represents information from a shard.
type ShardsInfo struct {
	Total      int                              `json:"total"`
	Successful int                              `json:"successful"`
	Failed     int                              `json:"failed"`
	Failures   []*ShardOperationFailedException `json:"failures,omitempty"`
	Skipped    int                              `json:"skipped,omitempty"`
}

type ShardOperationFailedException struct {
	Shard  int                    `json:"shard,omitempty"`
	Index  string                 `json:"index,omitempty"`
	Status string                 `json:"status,omitempty"`
	Reason map[string]interface{} `json:"reason,omitempty"`

	// TODO(oe) Do we still have those?
	Node string `json:"_node,omitempty"`
	// TODO(oe) Do we still have those?
	Primary bool `json:"primary,omitempty"`
}

type BroadcastResponse struct {
	Shards     *ShardsInfo                      `json:"_shards,omitempty"`
	Total      int                              `json:"total"`
	Successful int                              `json:"successful"`
	Failed     int                              `json:"failed"`
	Failures   []*ShardOperationFailedException `json:"failures,omitempty"`
}

// FailedNodeException returns an error on the node level.
type FailedNodeException struct {
	*ErrorDetails
	NodeId string `json:"node_id"`
}
