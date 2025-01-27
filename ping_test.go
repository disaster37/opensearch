// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"context"
	"net/http"
	"testing"
)

func TestPingGet(t *testing.T) {
	client := setupTestClientAndCreateIndex(t)

	res, code, err := client.Ping("https://opensearch.svc:9200").Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if code != http.StatusOK {
		t.Errorf("expected status code = %d; got %d", http.StatusOK, code)
	}
	if res == nil {
		t.Fatalf("expected to return result, got: %v", res)
	}
	if res.Name == "" {
		t.Errorf("expected Name != %q; got %q", "", res.Name)
	}
	if res.Version.Number == "" {
		t.Errorf("expected Version.Number != %q; got %q", "", res.Version.Number)
	}
}

func TestPingHead(t *testing.T) {
	client := setupTestClientAndCreateIndex(t)

	res, code, err := client.Ping("https://opensearch.svc:9200").HttpHeadOnly(true).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if code != http.StatusOK {
		t.Errorf("expected status code = %d; got %d", http.StatusOK, code)
	}
	if res != nil {
		t.Errorf("expected not to return result, got: %v", res)
	}
}

func TestPingHeadFailure(t *testing.T) {
	client := setupTestClientAndCreateIndex(t)

	res, code, err := client.
		Ping("http://127.0.0.1:9299").
		HttpHeadOnly(true).
		Do(context.TODO())
	if err == nil {
		t.Error("expected error, got nil")
	}
	if code == http.StatusOK {
		t.Errorf("expected status code != %d; got %d", http.StatusOK, code)
	}
	if res != nil {
		t.Errorf("expected not to return result, got: %v", res)
	}
}
