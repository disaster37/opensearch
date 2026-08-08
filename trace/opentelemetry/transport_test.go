package opentelemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"

	"github.com/disaster37/opensearch/v4"
	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func setupTracerProvider(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prev)
	})
	return recorder
}

func newTestServer(status int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
	}))
}

func makeRequest(t *testing.T, client *resty.Client, method, url string) *resty.Response {
	t.Helper()
	var resp *resty.Response
	var err error
	switch method {
	case http.MethodPost:
		resp, err = client.R().SetContext(context.Background()).Post(url)
	default:
		resp, err = client.R().SetContext(context.Background()).Get(url)
	}
	require.NoError(t, err)
	return resp
}

func TestMiddleware_ReturnsNonNil(t *testing.T) {
	before, after := Middleware("test-tracer")
	assert.NotNil(t, before, "request middleware should not be nil")
	assert.NotNil(t, after, "response middleware should not be nil")
}

func TestMiddleware_EmptyTracerName_UsesDefault(t *testing.T) {
	recorder := setupTracerProvider(t)
	before, _ := Middleware("")

	server := newTestServer(http.StatusOK)
	defer server.Close()

	client := resty.New()
	client.OnBeforeRequest(before)
	makeRequest(t, client, http.MethodGet, server.URL+"/test")

	started := recorder.Started()
	require.Len(t, started, 1)
	assert.Equal(t, "opensearch.request", started[0].Name())

	atts := started[0].Attributes()
	m := attrsMap(atts)
	assert.Contains(t, m["http.url"].AsString(), "/test")
	assert.Equal(t, "GET", m["http.method"].AsString())
}

func TestRequestMiddleware_CreatesSpanWithAttributes(t *testing.T) {
	recorder := setupTracerProvider(t)
	before, _ := Middleware("test-tracer")

	server := newTestServer(http.StatusOK)
	defer server.Close()

	client := resty.New()
	client.OnBeforeRequest(before)
	makeRequest(t, client, http.MethodGet, server.URL+"/my-index/_search")

	started := recorder.Started()
	require.Len(t, started, 1, "before middleware should create exactly one span")

	span := started[0]
	assert.Equal(t, "opensearch.request", span.Name())
	assert.True(t, span.IsRecording(), "span should still be recording after before middleware")

	m := attrsMap(span.Attributes())
	assert.Equal(t, server.URL+"/my-index/_search", m["http.url"].AsString())
	assert.Equal(t, "GET", m["http.method"].AsString())
}

func TestRequestMiddleware_SetsSpanInContext(t *testing.T) {
	setupTracerProvider(t)

	var capturedHolder *spanHolder
	before, _ := Middleware("test-tracer")

	server := newTestServer(http.StatusOK)
	defer server.Close()

	client := resty.New()
	client.OnBeforeRequest(before)
	client.OnAfterResponse(func(_ *resty.Client, resp *resty.Response) error {
		holderIface := resp.Request.Context().Value(contextKey{})
		if holderIface != nil {
			if h, ok := holderIface.(*spanHolder); ok {
				capturedHolder = h
			}
		}
		return nil
	})
	makeRequest(t, client, http.MethodGet, server.URL+"/test")

	require.NotNil(t, capturedHolder, "spanHolder should be stored in the request context")
	assert.NotNil(t, capturedHolder.span, "holder must contain a non-nil span")
	capturedHolder.endSpan()
}

func TestResponseMiddleware_ReturnsNil_WhenNoSpanInContext(t *testing.T) {
	_, after := Middleware("test-tracer")

	server := newTestServer(http.StatusOK)
	defer server.Close()

	client := resty.New()

	resp, err := client.R().SetContext(context.Background()).Get(server.URL + "/test")
	require.NoError(t, err)

	err = after(client, resp)
	assert.NoError(t, err, "response middleware should return nil when no span in context")
}

func TestResponseMiddleware_NoCrash_WithSpanInContext(t *testing.T) {
	setupTracerProvider(t)
	before, after := Middleware("test-tracer")

	server := newTestServer(http.StatusOK)
	defer server.Close()

	client := resty.New()
	client.OnBeforeRequest(before)

	resp, err := client.R().SetContext(context.Background()).Get(server.URL + "/test")
	require.NoError(t, err)

	err = after(client, resp)
	assert.NoError(t, err, "response middleware should return nil")
}

func TestResponseMiddleware_NoCrash_ErrorStatus(t *testing.T) {
	setupTracerProvider(t)
	before, after := Middleware("test-tracer")

	server := newTestServer(http.StatusInternalServerError)
	defer server.Close()

	client := resty.New()
	client.OnBeforeRequest(before)

	resp, err := client.R().SetContext(context.Background()).Get(server.URL + "/test")
	require.NoError(t, err)
	assert.True(t, resp.IsError())

	err = after(client, resp)
	assert.NoError(t, err)
}

func TestResponseMiddleware_EndsSpanAndSetsStatusCode(t *testing.T) {
	recorder := setupTracerProvider(t)
	before, after := Middleware("test-tracer")

	server := newTestServer(http.StatusOK)
	defer server.Close()

	client := resty.New()
	client.OnBeforeRequest(before)
	client.OnAfterResponse(after)

	resp, err := client.R().SetContext(context.Background()).Get(server.URL + "/test")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode())

	ended := recorder.Ended()
	require.Len(t, ended, 1, "span should be ended by the after middleware")

	m := attrsMap(ended[0].Attributes())
	statusCode, hasStatusCode := m["http.status_code"]
	assert.True(t, hasStatusCode, "status_code attribute should be set")
	assert.Equal(t, int64(http.StatusOK), statusCode.AsInt64())
}

func TestResponseMiddleware_SetsErrorStatusOnError(t *testing.T) {
	recorder := setupTracerProvider(t)
	before, after := Middleware("test-tracer")

	server := newTestServer(http.StatusInternalServerError)
	defer server.Close()

	client := resty.New()
	client.OnBeforeRequest(before)
	client.OnAfterResponse(after)

	resp, err := client.R().SetContext(context.Background()).Get(server.URL + "/test")
	require.NoError(t, err)
	assert.True(t, resp.IsError())

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, codes.Error, ended[0].Status().Code,
		"span status should be Error for 5xx responses")
}

func TestRequestMiddleware_PostMethod(t *testing.T) {
	recorder := setupTracerProvider(t)
	before, _ := Middleware("test-tracer")

	server := newTestServer(http.StatusCreated)
	defer server.Close()

	client := resty.New()
	client.OnBeforeRequest(before)

	resp, err := client.R().
		SetContext(context.Background()).
		SetBody(`{"query":{"match_all":{}}}`).
		Post(server.URL + "/my-index/_search")

	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode())

	started := recorder.Started()
	require.Len(t, started, 1)

	m := attrsMap(started[0].Attributes())
	assert.Equal(t, server.URL+"/my-index/_search", m["http.url"].AsString())
	assert.Equal(t, "POST", m["http.method"].AsString())
}

func TestRequestMiddleware_MultipleRequestsCreateMultipleSpans(t *testing.T) {
	recorder := setupTracerProvider(t)
	before, _ := Middleware("test-tracer")

	server := newTestServer(http.StatusOK)
	defer server.Close()

	client := resty.New()
	client.OnBeforeRequest(before)

	for i := 0; i < 3; i++ {
		makeRequest(t, client, http.MethodGet, server.URL+"/test")
	}

	started := recorder.Started()
	assert.Len(t, started, 3)
}

func TestRedactURL_StripsCredentials(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "with user info",
			input:    "https://admin:secret@opensearch.svc:9200/_search",
			expected: "https://opensearch.svc:9200/_search",
		},
		{
			name:     "with token as username only",
			input:    "https://api-key-12345@host.example.com:9200/idx",
			expected: "https://host.example.com:9200/idx",
		},
		{
			name:     "without credentials",
			input:    "https://opensearch.svc:9200/_search?pretty=true",
			expected: "https://opensearch.svc:9200/_search?pretty=true",
		},
		{
			name:     "invalid url",
			input:    "not a url ://",
			expected: "not a url ://",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, opensearch.RedactURL(tc.input))
		})
	}
}

func TestSpanHolder_EndSpanIdempotent(t *testing.T) {
	recorder := setupTracerProvider(t)
	_, span := otel.Tracer("test").Start(context.Background(), "idempotent-test")

	holder := &spanHolder{span: span}

	holder.endSpan()

	holder.endSpan()
	holder.endSpan()

	ended := recorder.Ended()
	assert.Len(t, ended, 1, "sync.Once must ensure span.End() is invoked exactly once across multiple endSpan calls")

	var nilHolder *spanHolder
	assert.NotPanics(t, func() { nilHolder.endSpan() }, "endSpan on a nil receiver must be safe")
}

func TestSpanHolder_EndSpanConcurrent(t *testing.T) {
	setupTracerProvider(t)
	_, span := otel.Tracer("test").Start(context.Background(), "concurrent-test")

	holder := &spanHolder{span: span}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			holder.endSpan()
		}()
	}
	wg.Wait()
}

func TestRequestMiddleware_RedactsURLInSpan(t *testing.T) {
	recorder := setupTracerProvider(t)
	before, _ := Middleware("test-tracer")

	client := resty.New()
	client.OnBeforeRequest(before)

	fakeURL := "https://admin:secret@host.example.com:9200/my-index/_search"
	req := client.R().SetContext(context.Background())
	req.URL = fakeURL
	req.Method = http.MethodGet

	_ = before(client, req)

	started := recorder.Started()
	require.Len(t, started, 1)

	m := attrsMap(started[0].Attributes())
	recordedURL := m["http.url"].AsString()
	assert.NotContains(t, recordedURL, "secret", "password must not appear in span attributes")
	assert.NotContains(t, recordedURL, "admin", "username must not appear in span attributes")
	assert.Equal(t, "https://host.example.com:9200/my-index/_search", recordedURL,
		"the user info must be fully stripped, leaving host and path intact")
}

func TestRequestMiddleware_FinalizerEndsSpanOnFailedRequest(t *testing.T) {
	recorder := setupTracerProvider(t)
	before, _ := Middleware("test-tracer")

	server := newTestServer(http.StatusOK)
	serverURL := server.URL
	server.Close()

	client := resty.New()
	client.OnBeforeRequest(before)

	func() {
		req := client.R().SetContext(context.Background())
		_, _ = client.R().SetContext(context.Background()).Get(serverURL + "/test")
		_ = req
	}()

	for i := 0; i < 5; i++ {
		runtime.GC()
		runtime.GC()
	}

	started := recorder.Started()
	require.NotEmpty(t, started)

	ended := recorder.Ended()
	assert.NotEmpty(t, ended, "GC finalizer should end the span when no response middleware fires")
}

func attrsMap(atts []attribute.KeyValue) map[attribute.Key]attribute.Value {
	m := make(map[attribute.Key]attribute.Value, len(atts))
	for _, a := range atts {
		m[a.Key] = a.Value
	}
	return m
}
