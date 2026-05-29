package opensearch

import (
	"crypto/tls"
	"crypto/x509"
	"time"

	"github.com/disaster37/opensearch/v3/api"
	"github.com/disaster37/opensearch/v3/types"
	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

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
}

// DefaultClient is the default [Client] implementation returned by [New].
type DefaultClient struct {
	client    *resty.Client
	logger    *logrus.Entry
	document  api.DocumentService
	search    api.SearchService
	indices   api.IndicesService
	cluster   api.ClusterService
	nodes     api.NodesService
	cat       api.CatService
	ingest    api.IngestService
	snapshot  api.SnapshotService
	tasks     api.TasksService
	script    api.ScriptService
	security  api.SecurityService
	ism       api.IsmService
	sm        api.SmService
	alerting  api.AlertingService
	transform api.TransformService
	ccr       api.CcrService
	info      api.InfoService
	rollup    api.RollupService
}

// New creates a new [Client] connecting to an OpenSearch cluster.
//
// The returned client builds a resty HTTP client internally, applies TLS and
// authentication from cfg, and instantiates all 17 service interfaces.
//
// Example:
//
//	client, err := opensearch.New(&opensearch.Config{
//	    URL:      "https://localhost:9200",
//	    Username: "admin",
//	    Password: "admin",
//	}, logrus.NewEntry(logrus.StandardLogger()))
func New(cfg *Config, logger *logrus.Entry) (Client, error) {
	c := resty.New()

	if cfg.URL != "" {
		c.SetBaseURL(cfg.URL)
	}

	if cfg.Username != "" || cfg.Password != "" {
		c.SetBasicAuth(cfg.Username, cfg.Password)
	}

	if cfg.Timeout > 0 {
		c.SetTimeout(cfg.Timeout)
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

	return &DefaultClient{
		client:    c,
		logger:    logger,
		document:  api.NewDocumentService(c, logger),
		search:    api.NewSearchService(c, logger),
		indices:   api.NewIndicesService(c, logger),
		cluster:   api.NewClusterService(c, logger),
		nodes:     api.NewNodesService(c, logger),
		cat:       api.NewCatService(c, logger),
		ingest:    api.NewIngestService(c, logger),
		snapshot:  api.NewSnapshotService(c, logger),
		tasks:     api.NewTasksService(c, logger),
		script:    api.NewScriptService(c, logger),
		security:  api.NewSecurityService(c, logger),
		ism:       api.NewIsmService(c, logger),
		sm:        api.NewSmService(c, logger),
		alerting:  api.NewAlertingService(c, logger),
		transform: api.NewTransformService(c, logger),
		ccr:       api.NewCcrService(c, logger),
		info:      api.NewInfoService(c, logger),
		rollup:    api.NewRollupService(c, logger),
	}, nil
}

func (c *DefaultClient) RestyClient() *resty.Client      { return c.client }
func (c *DefaultClient) Document() api.DocumentService   { return c.document }
func (c *DefaultClient) Search() api.SearchService       { return c.search }
func (c *DefaultClient) Indices() api.IndicesService     { return c.indices }
func (c *DefaultClient) Cluster() api.ClusterService     { return c.cluster }
func (c *DefaultClient) Nodes() api.NodesService         { return c.nodes }
func (c *DefaultClient) Cat() api.CatService             { return c.cat }
func (c *DefaultClient) Ingest() api.IngestService       { return c.ingest }
func (c *DefaultClient) Snapshot() api.SnapshotService   { return c.snapshot }
func (c *DefaultClient) Tasks() api.TasksService         { return c.tasks }
func (c *DefaultClient) Script() api.ScriptService       { return c.script }
func (c *DefaultClient) Security() api.SecurityService   { return c.security }
func (c *DefaultClient) ISM() api.IsmService             { return c.ism }
func (c *DefaultClient) SM() api.SmService               { return c.sm }
func (c *DefaultClient) Alerting() api.AlertingService   { return c.alerting }
func (c *DefaultClient) Transform() api.TransformService { return c.transform }
func (c *DefaultClient) CCR() api.CcrService             { return c.ccr }
func (c *DefaultClient) Info() api.InfoService            { return c.info }
func (c *DefaultClient) Rollup() api.RollupService        { return c.rollup }
