// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opencensus

import (
	"context"
	"fmt"
	"net/http"

	"emperror.dev/errors"
	"go.opencensus.io/trace"
)

// Transport for tracing Elastic operations.
type Transport struct {
	rt                http.RoundTripper
	defaultAttributes []trace.Attribute
}

// Option signature for specifying options, e.g. WithRoundTripper.
type Option func(t *Transport)

// WithRoundTripper specifies the http.RoundTripper to call
// next after this transport. If it is nil (default), the
// transport will use http.DefaultTransport.
func WithRoundTripper(rt http.RoundTripper) Option {
	return func(t *Transport) {
		t.rt = rt
	}
}

// WithDefaultAttributes specifies default attributes to add
// to each span.
func WithDefaultAttributes(attrs ...trace.Attribute) Option {
	return func(t *Transport) {
		t.defaultAttributes = attrs
	}
}

// NewTransport specifies a transport that will trace Elastic
// and report back via OpenTracing.
func NewTransport(opts ...Option) *Transport {
	t := &Transport{}
	for _, o := range opts {
		o(t)
	}
	return t
}

// RoundTrip captures the request and starts an OpenTracing span
// for Elastic PerformRequest operation.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	_, span := trace.StartSpan(req.Context(), "opensearch:PerformRequest")
	attrs := append([]trace.Attribute(nil), t.defaultAttributes...)
	attrs = append(attrs,
		trace.StringAttribute("Component", "github.com/disaster37/opensearch/v2"),
		trace.StringAttribute("Method", req.Method),
		trace.StringAttribute("URL", req.URL.Redacted()),
		trace.StringAttribute("Hostname", req.URL.Hostname()),
		trace.Int64Attribute("Port", atoi64(req.URL.Port())),
	)
	span.AddAttributes(attrs...)

	var (
		resp *http.Response
		err  error
	)
	defer func() {
		setSpanStatus(span, err)
		span.End()
	}()
	if t.rt != nil {
		resp, err = t.rt.RoundTrip(req)
	} else {
		resp, err = http.DefaultTransport.RoundTrip(req)
	}
	return resp, err
}

// See https://github.com/opencensus-integrations/ocsql/blob/master/driver.go#L749
func setSpanStatus(span *trace.Span, err error) {
	var status trace.Status
	switch {
	case err == nil:
		status.Code = trace.StatusCodeOK
		span.SetStatus(status)
		return
	case err == context.Canceled:
		status.Code = trace.StatusCodeCancelled
	case err == context.DeadlineExceeded:
		status.Code = trace.StatusCodeDeadlineExceeded
	case isConnErr(err):
		status.Code = trace.StatusCodeUnavailable
	case isNotFound(err):
		status.Code = trace.StatusCodeNotFound
	case isConflict(err):
		status.Code = trace.StatusCodeFailedPrecondition
	case isForbidden(err):
		status.Code = trace.StatusCodePermissionDenied
	case isTimeout(err):
		status.Code = trace.StatusCodeResourceExhausted
	default:
		status.Code = trace.StatusCodeUnknown
	}
	status.Message = err.Error()
	span.SetStatus(status)
}

// Copied from opensearch to prevent cyclic dependencies.
type opensearchError struct {
	Status  int           `json:"status"`
	Details *errorDetails `json:"error,omitempty"`
}

// errorDetails encapsulate error details from Opensearch.
// It is used in e.g. opensearch.Error and opensearch.BulkResponseItem.
type errorDetails struct {
	Type         string                   `json:"type"`
	Reason       string                   `json:"reason"`
	ResourceType string                   `json:"resource.type,omitempty"`
	ResourceId   string                   `json:"resource.id,omitempty"`
	Index        string                   `json:"index,omitempty"`
	Phase        string                   `json:"phase,omitempty"`
	Grouped      bool                     `json:"grouped,omitempty"`
	CausedBy     map[string]interface{}   `json:"caused_by,omitempty"`
	RootCause    []*errorDetails          `json:"root_cause,omitempty"`
	FailedShards []map[string]interface{} `json:"failed_shards,omitempty"`
}

// Error returns a string representation of the error.
func (e *opensearchError) Error() string {
	if e.Details != nil && e.Details.Reason != "" {
		return fmt.Sprintf("opensearch: Error %d (%s): %s [type=%s]", e.Status, http.StatusText(e.Status), e.Details.Reason, e.Details.Type)
	}
	return fmt.Sprintf("opensearch: Error %d (%s)", e.Status, http.StatusText(e.Status))
}

// isConnErr returns true if the error indicates that Elastic could not
// find an Opensearch host to connect to.
func isConnErr(err error) bool {
	if err == nil {
		return false
	}
	if err.Error() == "no Opensearch node available" {
		return true
	}
	innerErr := errors.Cause(err)
	if innerErr == nil {
		return false
	}
	if innerErr.Error() == "no Opensearch node available" {
		return true
	}
	return false
}

// isNotFound returns true if the given error indicates that Opensearch
// returned HTTP status 404. The err parameter can be of type *opensearch.Error,
// opensearch.Error, *http.Response or int (indicating the HTTP status code).
func isNotFound(err interface{}) bool {
	return isStatusCode(err, http.StatusNotFound)
}

// isTimeout returns true if the given error indicates that Opensearch
// returned HTTP status 408. The err parameter can be of type *opensearch.Error,
// opensearch.Error, *http.Response or int (indicating the HTTP status code).
func isTimeout(err interface{}) bool {
	return isStatusCode(err, http.StatusRequestTimeout)
}

// isConflict returns true if the given error indicates that the Opensearch
// operation resulted in a version conflict. This can occur in operations like
// `update` or `index` with `op_type=create`. The err parameter can be of
// type *opensearch.Error, opensearch.Error, *http.Response or int (indicating the
// HTTP status code).
func isConflict(err interface{}) bool {
	return isStatusCode(err, http.StatusConflict)
}

// isForbidden returns true if the given error indicates that Opensearch
// returned HTTP status 403. This happens e.g. due to a missing license.
// The err parameter can be of type *opensearch.Error, opensearch.Error,
// *http.Response or int (indicating the HTTP status code).
func isForbidden(err interface{}) bool {
	return isStatusCode(err, http.StatusForbidden)
}

// isStatusCode returns true if the given error indicates that the Opensearch
// operation returned the specified HTTP status code. The err parameter can be of
// type *http.Response, *Error, Error, or int (indicating the HTTP status code).
func isStatusCode(err interface{}, code int) bool {
	switch e := err.(type) {
	case *http.Response:
		return e.StatusCode == code
	case *opensearchError:
		return e.Status == code
	case opensearchError:
		return e.Status == code
	case int:
		return e == code
	}
	return false
}
