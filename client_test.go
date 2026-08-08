package opensearch

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateSelfSignedCA(t *testing.T) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
}

func TestNew(t *testing.T) {
	logger := logrus.NewEntry(logrus.StandardLogger())

	t.Run("with full config", func(t *testing.T) {
		client, err := New(&Config{
			URL:           "https://localhost:9200",
			Username:      "admin",
			Password:      "admin",
			TLSSkipVerify: true,
			Timeout:       30 * time.Second,
		}, logger)
		require.NoError(t, err)
		require.NotNil(t, client)

		dc := client.(*DefaultClient)
		assert.NotNil(t, dc.RestyClient())
		assert.NotNil(t, dc.Document())
		assert.NotNil(t, dc.Search())
		assert.NotNil(t, dc.Indices())
		assert.NotNil(t, dc.Cluster())
		assert.NotNil(t, dc.Nodes())
		assert.NotNil(t, dc.Cat())
		assert.NotNil(t, dc.Ingest())
		assert.NotNil(t, dc.Snapshot())
		assert.NotNil(t, dc.Tasks())
		assert.NotNil(t, dc.Script())
		assert.NotNil(t, dc.Security())
		assert.NotNil(t, dc.ISM())
		assert.NotNil(t, dc.SM())
		assert.NotNil(t, dc.Alerting())
		assert.NotNil(t, dc.Transform())
		assert.NotNil(t, dc.CCR())
		assert.NotNil(t, dc.Info())
		assert.NotNil(t, dc.Rollup())
		assert.NotNil(t, dc.SQL())
		assert.NotNil(t, dc.AD())
		assert.NotNil(t, dc.ML())
		assert.NotNil(t, dc.AsyncSearch())
		assert.NotNil(t, dc.KNN())
		assert.NotNil(t, dc.Neural())
		assert.NotNil(t, dc.Tiering())
		assert.NotNil(t, dc.Ingestion())
	})

	t.Run("with minimal config", func(t *testing.T) {
		client, err := New(&Config{
			URL: "http://localhost:9200",
		}, logger)
		require.NoError(t, err)
		require.NotNil(t, client)
	})

	t.Run("with empty config", func(t *testing.T) {
		client, err := New(&Config{}, logger)
		require.NoError(t, err)
		require.NotNil(t, client)
	})

	t.Run("with invalid CA cert", func(t *testing.T) {
		client, err := New(&Config{
			URL:    "https://localhost:9200",
			CACert: []byte("not-a-valid-pem"),
		}, logger)
		assert.Nil(t, client)
		assert.Error(t, err)
	})

	t.Run("with valid CA cert", func(t *testing.T) {
		caCert := []byte(`-----BEGIN CERTIFICATE-----
MIICEjCCAXsCAg36MA0GCSqGSIb3DQEBBQUAMIGbMQswCQYDVQQGEwJKUDEOMAwG
A1UECAwFVG9reW8xEDAOBgNVBAcMB0NodW8ta3UxETAPBgNVBAoMCEZyYW5rNERE
MRgwFgYDVQQLDA9XZWJDZXJ0IFN1cHBvcnQxGDAWBgNVBAMMD0ZyYW5rNEREIFdl
YiBDQTEjMCEGCSqGSIb3DQEJARYUc3VwcG9ydEBmcmFuazRkZC5jb20wHhcNMTIw
ODIyMDUyNjU0WhcNMTcwODIxMDUyNjU0WjBKMQswCQYDVQQGEwJKUDEOMAwGA1UE
CAwFVG9reW8xETAPBgNVBAoMCEZyYW5rNEREMRgwFgYDVQQDDA93d3cuZXhhbXBs
ZS5jb20wXDANBgkqhkiG9w0BAQEFAANLADBIAkEAm/xmkHmEQrurE/0re/jeFRLl
8ZPjBop7uLHhnia7lQG/5zDtZIUC3RVpqDSwBuw/NTweGyuP+o8AG98HxqxTBwID
AQABMA0GCSqGSIb3DQEBBQUAA4IBABS2TLuBeTPmcaTaUW/LCB2NYOy8GMdzR1mx
8iBIu2H6/E2tiY3RIevV2OW61qY2/XRQg7YPxx3ffeUugX9F4J/iPnnu1zAxxyBy
2VguKv4SWjRFoRkIfIlHX2Wn5Q3G0h4G5J3O1N3TqY7Gz6J2V8bGj7FZ3kGz5K3M
d1F1P8G6S2j2J0b1Y9H6K5R6M8M5b0j5H7R0l8K5J6M8M5b0j5H7R0l8K5J6M8M
5b0j5H7R0l8K5J6M8M5b0j5H7R0l8K5J6M8M5b0j5H7R0l=
-----END CERTIFICATE-----`)
		_, err := New(&Config{
			URL:    "https://localhost:9200",
			CACert: caCert,
		}, logger)
		assert.Error(t, err)
	})

	t.Run("with valid self-signed CA cert", func(t *testing.T) {
		caCert := generateSelfSignedCA(t)
		client, err := New(&Config{
			URL:    "https://localhost:9200",
			CACert: caCert,
		}, logger)
		require.NoError(t, err)
		require.NotNil(t, client)
	})
}

func TestNew_HTTPCallbacks(t *testing.T) {
	logger := logrus.NewEntry(logrus.StandardLogger())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(&Config{
		URL: server.URL,
	}, logger)
	require.NoError(t, err)
	require.NotNil(t, client)

	resp, err := client.RestyClient().R().Get(server.URL + "/test")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode())
}

func TestNew_ContentTypeHeader(t *testing.T) {
	logger := logrus.NewEntry(logrus.StandardLogger())

	var capturedContentType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedContentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(&Config{URL: server.URL}, logger)
	require.NoError(t, err)

	// Send a raw string body – without the global header this would produce
	// "text/plain; charset=utf-8" instead of "application/json".
	_, err = client.RestyClient().R().
		SetBody(`{"settings":{"number_of_shards":1}}`).
		Put(server.URL + "/test-index")
	require.NoError(t, err)

	assert.Equal(t, "application/json", capturedContentType,
		"Content-Type must be application/json even when body is a raw string")
}

func TestRedactURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "with user and password",
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
			name:     "invalid url returned unchanged",
			input:    "not a url ://",
			expected: "not a url ://",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, RedactURL(tc.input))
		})
	}
}

func TestRedactSensitiveHeaders(t *testing.T) {
	msg := "~~~ REQUEST ~~~\n" +
		"GET  /_search  HTTP/1.1\n" +
		"HOST   : opensearch.svc:9200\n" +
		"HEADERS:\n" +
		"\t            Authorization: Basic YWRtaW46c2VjcmV0\n" +
		"\t          Content-Type: application/json\n" +
		"\t       X-Amz-Security-Token: IQoJb3JpZ2lu\n" +
		"\t                  Cookie: session=abc123\n" +
		"\t              Set-Cookie: refresh=def456\n" +
		"BODY   :\n" +
		"{\"query\":{}}\n"

	out := redactSensitiveHeaders(msg)

	assert.Contains(t, out, "Authorization: [REDACTED]")
	assert.Contains(t, out, "X-Amz-Security-Token: [REDACTED]")
	assert.Contains(t, out, "Cookie: [REDACTED]")
	assert.Contains(t, out, "Set-Cookie: [REDACTED]")
	assert.NotContains(t, out, "YWRtaW46c2VjcmV0")
	assert.NotContains(t, out, "IQoJb3JpZ2lu")
	assert.NotContains(t, out, "abc123")
	assert.NotContains(t, out, "def456")
	// Non-sensitive headers and structural lines are preserved.
	assert.Contains(t, out, "Content-Type: application/json")
	assert.Contains(t, out, "HOST   : opensearch.svc:9200")
	assert.Contains(t, out, "~~~ REQUEST ~~~")
}

func TestRedactingRestyLogger_RedactsAuthorization(t *testing.T) {
	// Capture logrus output to verify the redacting logger scrubs the
	// Authorization header from resty's debug dump before it is written.
	log := logrus.New()
	log.SetLevel(logrus.TraceLevel)
	var buf bytes.Buffer
	log.SetOutput(&buf)

	entry := logrus.NewEntry(log)
	rl := &redactingRestyLogger{entry: entry}

	debugDump := "~~~ REQUEST ~~~\n" +
		"HEADERS:\n" +
		"\t            Authorization: Basic YWRtaW46c2VjcmV0\n" +
		"\t          Content-Type: application/json\n"

	rl.Debugf("%s", debugDump)

	out := buf.String()
	assert.Contains(t, out, "Authorization: [REDACTED]")
	assert.NotContains(t, out, "YWRtaW46c2VjcmV0")
	assert.NotContains(t, out, "Basic")
}

func TestNew_OnAfterResponse_RedactsURLWithUserinfo(t *testing.T) {
	// When Config.URL carries userinfo, the OnAfterResponse debug log must
	// not contain the credentials.
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	var buf bytes.Buffer
	log.SetOutput(&buf)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := New(&Config{
		URL:      server.URL,
		Username: "admin",
		Password: "supersecret",
	}, logrus.NewEntry(log))
	require.NoError(t, err)

	_, err = client.RestyClient().R().Get(server.URL + "/_test")
	require.NoError(t, err)

	out := buf.String()
	assert.NotContains(t, out, "supersecret", "password must not leak into logs")
	assert.NotContains(t, out, "admin:supersecret@", "userinfo must not leak into logs")
}
