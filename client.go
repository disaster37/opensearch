package opensearch

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"strings"
	"time"

	"github.com/disaster37/opensearch/v4/api"
	"github.com/disaster37/opensearch/v4/types"
	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

// DefaultRetryConditions returns the default retry conditions that handle:
// - Network errors (no response received)
// - 429 Too Many Requests status code
// - 5xx server error status codes (excluding 501 Not Implemented)
//
// These conditions are automatically applied when RetryCount > 0, but you can
// use this function to build custom retry logic that includes the defaults.
func DefaultRetryConditions() []resty.RetryConditionFunc {
	return []resty.RetryConditionFunc{
		func(res *resty.Response, err error) bool {
			if res == nil {
				// Retry on network/transport errors including connection reset
				if err != nil {
					errStr := err.Error()
					// Common network errors that should be retried
					networkErrors := []string{
						"connection reset by peer",
						"connection refused",
						"timeout",
						"i/o timeout",
						"network is unreachable",
						"broken pipe",
						"EOF",
					}
					for _, netErr := range networkErrors {
						if strings.Contains(strings.ToLower(errStr), netErr) {
							return true
						}
					}
					// Also retry on any other non-nil error when no response
					return true
				}
				return false
			}
			status := res.StatusCode()
			return status == 429 || (status >= 500 && status != 501)
		},
	}
}

// PITSearchRetryConditions returns retry conditions specifically optimized for
// Point-in-Time (PIT) search queries and other long-running OpenSearch operations.
// Includes default conditions plus additional OpenSearch-specific scenarios.
func PITSearchRetryConditions() []resty.RetryConditionFunc {
	defaultConds := DefaultRetryConditions()
	return append(
		defaultConds,
		// Retry on OpenSearch-specific errors that may be transient
		func(res *resty.Response, err error) bool {
			if res == nil {
				return false
			}
			// Check for specific OpenSearch error types in response body
			// Common transient errors: "search_phase_execution_exception",
			// "too_many_buckets_exception", "circuit_breaking_exception"
			body := string(res.Body())
			return strings.Contains(body, "search_phase_execution_exception") ||
				strings.Contains(body, "too_many_buckets_exception") ||
				strings.Contains(body, "circuit_breaking_exception")
		},
	)
}

// Client is the main entry point for all OpenSearch operations.
//
// Obtain a Client via [New]. All API groups are accessible as methods on this
// interface:
//
//	client, err := opensearch.New(&opensearch.Config{...}, logger)
//
//	client.Document().Index(ctx, "idx", doc, "id", nil)
//	client.Search().Search(ctx, []string{"idx"}, query, nil)
//	client.Indices().Create(ctx, "new-idx", settings)
//	client.Cluster().Health(ctx, nil, nil)
type Client interface {
	// RestyClient returns the underlying resty client for middleware
	// configuration (e.g., adding OpenTelemetry tracing).
	RestyClient() *resty.Client

	Document() api.DocumentService
	Search() api.SearchService
	Indices() api.IndicesService
	Cluster() api.ClusterService
	Nodes() api.NodesService
	Cat() api.CatService
	Ingest() api.IngestService
	Snapshot() api.SnapshotService
	Tasks() api.TasksService
	Script() api.ScriptService
	Security() api.SecurityService
	ISM() api.IsmService
	SM() api.SmService
	Alerting() api.AlertingService
	Transform() api.TransformService
	CCR() api.CcrService
	Info() api.InfoService
	Rollup() api.RollupService
	ML() api.MlService
	SQL() api.SqlService
	AD() api.AdService
	AsyncSearch() api.AsyncSearchService
	KNN() api.KnnService
	Neural() api.NeuralService
}

// Config holds configuration for the OpenSearch client.
type Config struct {
	// URL is the OpenSearch cluster address. Required.
	// Examples: "https://localhost:9200", "http://cluster:9200"
	URL string

	// Username for HTTP basic authentication.
	Username string

	// Password for HTTP basic authentication.
	Password string

	// TLSSkipVerify disables TLS certificate verification.
	// WARNING: Only set to true for development. For production, use CACert.
	TLSSkipVerify bool

	// CACert is a PEM-encoded CA certificate used to verify the server.
	// If empty and TLSSkipVerify is false, the system cert pool is used.
	CACert []byte

	// Timeout is the HTTP request timeout. Zero means no timeout.
	Timeout time.Duration

	// IdleConnTimeout is the maximum amount of time an idle (keep-alive)
	// connection is kept in the pool before being closed.
	// Set it below the keep-alive timeout of any intermediate proxy
	// (e.g. an nginx ingress, default 75s) to avoid reusing a connection
	// the peer has already closed, which surfaces as
	// "connection reset by peer". Zero keeps the Go default (90s).
	IdleConnTimeout time.Duration

	// RetryCount is the maximum number of retry attempts for failed requests.
	// Default is 0 (no retries). Set to a positive integer to enable retries.
	RetryCount int

	// RetryWaitTime is the minimum wait time between retry attempts.
	// Default is 100ms if not specified.
	RetryWaitTime time.Duration

	// RetryMaxWaitTime is the maximum wait time between retry attempts.
	// Default is 2s if not specified.
	RetryMaxWaitTime time.Duration

	// RetryConditions are custom functions that determine if a request should be retried.
	// By default, resty retries on network errors, 429 Too Many Requests, and 5xx server errors.
	// Add custom conditions to extend the default behavior.
	RetryConditions []resty.RetryConditionFunc
}

// DefaultClient is the default [Client] implementation returned by [New].
type DefaultClient struct {
	client      *resty.Client
	logger      *logrus.Entry
	document    api.DocumentService
	search      api.SearchService
	indices     api.IndicesService
	cluster     api.ClusterService
	nodes       api.NodesService
	cat         api.CatService
	ingest      api.IngestService
	snapshot    api.SnapshotService
	tasks       api.TasksService
	script      api.ScriptService
	security    api.SecurityService
	ism         api.IsmService
	sm          api.SmService
	alerting    api.AlertingService
	transform   api.TransformService
	ccr         api.CcrService
	info        api.InfoService
	rollup      api.RollupService
	ml          api.MlService
	sql         api.SqlService
	ad          api.AdService
	asyncSearch api.AsyncSearchService
	knn         api.KnnService
	neural      api.NeuralService
}

// New creates a new [Client] connecting to an OpenSearch cluster.
//
// The returned client builds a resty HTTP client internally, applies TLS and
// authentication from cfg, and instantiates all 18 service interfaces.
//
// Example with retry configuration for long-running queries (like Point-in-Time searches):
//
//	client, err := opensearch.New(&opensearch.Config{
//	    URL:      "https://localhost:9200",
//	    Username: "admin",
//	    Password: "admin",
//	    RetryCount: 3,
//	    RetryWaitTime: 500 * time.Millisecond,
//	    RetryMaxWaitTime: 5 * time.Second,
//	    RetryConditions: opensearch.PITSearchRetryConditions(),
//	}, logrus.NewEntry(logrus.StandardLogger()))
func New(cfg *Config, logger *logrus.Entry) (Client, error) {
	c := resty.New()

	// Always send application/json so that string/[]byte bodies are not sent
	// with the resty default of "text/plain; charset=utf-8".
	c.SetHeader("Content-Type", "application/json")

	if cfg.URL != "" {
		c.SetBaseURL(cfg.URL)
	}

	if cfg.Username != "" || cfg.Password != "" {
		c.SetBasicAuth(cfg.Username, cfg.Password)
	}

	if cfg.Timeout > 0 {
		c.SetTimeout(cfg.Timeout)
	}

	if cfg.IdleConnTimeout == 0 {
		cfg.IdleConnTimeout = 60 * time.Second
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.TLSSkipVerify,
	}

	if len(cfg.CACert) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(cfg.CACert) {
			return nil, &types.OpenSearchError{
				Details: &types.OpenSearchErrorDetails{Reason: "failed to parse CA certificate"},
			}
		}
		tlsConfig.RootCAs = pool
	}

	c.SetTLSClientConfig(tlsConfig)

	// Customize the underlying transport so the connection pool can be
	// aligned with upstream proxies. Cloning the default transport keeps
	// resty/net-http defaults (proxy from env, dialer, HTTP/2, etc.).
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	if cfg.IdleConnTimeout > 0 {
		transport.IdleConnTimeout = cfg.IdleConnTimeout
	}
	c.SetTransport(transport)

	c.OnBeforeRequest(func(_ *resty.Client, req *resty.Request) error {
		logger.WithFields(logrus.Fields{
			"http_method": req.Method,
			"http_path":   req.URL,
		}).Debug("Sending request to OpenSearch")
		return nil
	})
	c.OnAfterResponse(func(_ *resty.Client, resp *resty.Response) error {
		logger.WithFields(logrus.Fields{
			"http_method":   resp.Request.Method,
			"http_path":     resp.Request.URL,
			"http_status":   resp.StatusCode(),
			"http_duration": resp.Time().String(),
		}).Debug("Received response from OpenSearch")
		return nil
	})

	// Pu client on debug if logger is set to debug level or lower.
	if logger.Logger.IsLevelEnabled(logrus.DebugLevel) {
		c.SetDebug(true)
	}

	// Configure retry settings if specified
	if cfg.RetryCount > 0 {
		c.SetRetryCount(cfg.RetryCount)

		if cfg.RetryWaitTime > 0 {
			c.SetRetryWaitTime(cfg.RetryWaitTime)
		}

		if cfg.RetryMaxWaitTime > 0 {
			c.SetRetryMaxWaitTime(cfg.RetryMaxWaitTime)
		}

		if len(cfg.RetryConditions) > 0 {
			for _, condition := range cfg.RetryConditions {
				c.AddRetryCondition(condition)
			}
		}
	}

	return &DefaultClient{
		client:      c,
		logger:      logger,
		document:    api.NewDocumentService(c, logger),
		search:      api.NewSearchService(c, logger),
		indices:     api.NewIndicesService(c, logger),
		cluster:     api.NewClusterService(c, logger),
		nodes:       api.NewNodesService(c, logger),
		cat:         api.NewCatService(c, logger),
		ingest:      api.NewIngestService(c, logger),
		snapshot:    api.NewSnapshotService(c, logger),
		tasks:       api.NewTasksService(c, logger),
		script:      api.NewScriptService(c, logger),
		security:    api.NewSecurityService(c, logger),
		ism:         api.NewIsmService(c, logger),
		sm:          api.NewSmService(c, logger),
		alerting:    api.NewAlertingService(c, logger),
		transform:   api.NewTransformService(c, logger),
		ccr:         api.NewCcrService(c, logger),
		info:        api.NewInfoService(c, logger),
		rollup:      api.NewRollupService(c, logger),
		ml:          api.NewMlService(c, logger),
		sql:         api.NewSqlService(c, logger),
		ad:          api.NewAdService(c, logger),
		asyncSearch: api.NewAsyncSearchService(c, logger),
		knn:         api.NewKnnService(c, logger),
		neural:      api.NewNeuralService(c, logger),
	}, nil
}

func (c *DefaultClient) RestyClient() *resty.Client          { return c.client }
func (c *DefaultClient) Document() api.DocumentService       { return c.document }
func (c *DefaultClient) Search() api.SearchService           { return c.search }
func (c *DefaultClient) Indices() api.IndicesService         { return c.indices }
func (c *DefaultClient) Cluster() api.ClusterService         { return c.cluster }
func (c *DefaultClient) Nodes() api.NodesService             { return c.nodes }
func (c *DefaultClient) Cat() api.CatService                 { return c.cat }
func (c *DefaultClient) Ingest() api.IngestService           { return c.ingest }
func (c *DefaultClient) Snapshot() api.SnapshotService       { return c.snapshot }
func (c *DefaultClient) Tasks() api.TasksService             { return c.tasks }
func (c *DefaultClient) Script() api.ScriptService           { return c.script }
func (c *DefaultClient) Security() api.SecurityService       { return c.security }
func (c *DefaultClient) ISM() api.IsmService                 { return c.ism }
func (c *DefaultClient) SM() api.SmService                   { return c.sm }
func (c *DefaultClient) Alerting() api.AlertingService       { return c.alerting }
func (c *DefaultClient) Transform() api.TransformService     { return c.transform }
func (c *DefaultClient) CCR() api.CcrService                 { return c.ccr }
func (c *DefaultClient) Info() api.InfoService               { return c.info }
func (c *DefaultClient) Rollup() api.RollupService           { return c.rollup }
func (c *DefaultClient) ML() api.MlService                   { return c.ml }
func (c *DefaultClient) SQL() api.SqlService                 { return c.sql }
func (c *DefaultClient) AD() api.AdService                   { return c.ad }
func (c *DefaultClient) AsyncSearch() api.AsyncSearchService { return c.asyncSearch }
func (c *DefaultClient) KNN() api.KnnService                 { return c.knn }
func (c *DefaultClient) Neural() api.NeuralService           { return c.neural }
