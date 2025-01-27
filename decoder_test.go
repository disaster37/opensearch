// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
)

type decoder struct {
	N int64
}

func (d *decoder) Decode(data []byte, v interface{}) error {
	atomic.AddInt64(&d.N, 1)
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode(v)
}

func TestDecoder(t *testing.T) {
	dec := &decoder{}
	client := setupTestClientAndCreateIndex(t, SetDecoder(dec), SetMaxRetries(0))

	tweet := tweet{User: "olivere", Message: "Welcome to Golang and Opensearch."}

	// Add a document
	indexResult, err := client.Index().
		Index(testIndexName).
		Id("1").
		BodyJson(&tweet).
		Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if indexResult == nil {
		t.Errorf("expected result to be != nil; got: %v", indexResult)
	}
	if dec.N == 0 {
		t.Errorf("expected at least 1 call of decoder; got: %d", dec.N)
	}
}
