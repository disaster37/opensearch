// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package opensearch

// RuntimeMappings specify fields that are evaluated at query time.
//
// See https://www.opensearch.co/guide/en/opensearchsearch/reference/7.14/runtime.html
// for details.
type RuntimeMappings map[string]interface{}

// Source deserializes the runtime mappings.
func (m *RuntimeMappings) Source() (interface{}, error) {
	return m, nil
}
