package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

func testLogger() *logrus.Entry {
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	return logrus.NewEntry(logger)
}

func restyClient(ts *httptest.Server) *resty.Client {
	c := resty.New()
	c.SetBaseURL(ts.URL)
	return c
}

func deadClient() *resty.Client {
	return resty.New().
		SetBaseURL("http://127.0.0.1:1").
		SetTimeout(100 * time.Millisecond)
}

func jsonResponse(w http.ResponseWriter, statusCode int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_, _ = fmt.Fprint(w, body)
}

func errServer(status int) *httptest.Server {
	errJSON := `{"error":{"type":"test_error","reason":"test error"},"status":` + fmt.Sprint(status) + `}`
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, errJSON)
	}))
}

func badJSONServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = fmt.Fprint(w, `not json at all`)
	}))
}
