package api

import (
	"fmt"

	json "github.com/goccy/go-json"
	"github.com/sirupsen/logrus"

	"github.com/disaster37/opensearch/v3/types"
	"github.com/go-resty/resty/v2"
)

func parseErrorResponse(resp *resty.Response) error {
	e := &types.OpenSearchError{Status: resp.StatusCode()}
	if resp.Body() != nil {
		_ = json.Unmarshal(resp.Body(), e)
		if e.Status == 0 {
			e.Status = resp.StatusCode()
		}
	}
	return e
}

// logAndReturnError parses the OpenSearch error response, logs it at Error level
// with the HTTP method, path and status code, and returns the parsed error.
func logAndReturnError(logger *logrus.Entry, resp *resty.Response) error {
	e := parseErrorResponse(resp)
	logger.WithFields(logrus.Fields{
		"http_method": resp.Request.Method,
		"http_path":   resp.Request.URL,
		"http_status": resp.StatusCode(),
	}).WithError(e).Error("OpenSearch request failed")
	return e
}

// wrapNetworkError logs a network-level failure at Error level and returns
// the original error unchanged, preserving the error chain for errors.Is / errors.As.
func wrapNetworkError(logger *logrus.Entry, err error) error {
	logger.WithError(err).Error("HTTP request failed")
	return err
}

// wrapUnmarshalError logs a JSON deserialization failure at Error level,
// including a truncated response body for debugging, and returns a wrapped error
// with endpoint context (HTTP method + path).
func wrapUnmarshalError(logger *logrus.Entry, resp *resty.Response, err error) error {
	body := string(resp.Body())
	if len(body) > 500 {
		body = body[:500] + "..."
	}
	logger.WithFields(logrus.Fields{
		"http_method":   resp.Request.Method,
		"http_path":     resp.Request.URL,
		"http_status":   resp.StatusCode(),
		"response_body": body,
	}).WithError(err).Error("Failed to unmarshal response")
	return fmt.Errorf("unmarshal response from %s %s: %w", resp.Request.Method, resp.Request.URL, err)
}
