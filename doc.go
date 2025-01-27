// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

/*
package opensearch provides an interface to the Opensearch server
(https://www.opensearch.co/products/opensearchsearch).

The first thing you do is to create a Client. If you have Opensearch
installed and running with its default settings
(i.e. available at http://127.0.0.1:9200), all you need to do is:

	client, err := opensearch.NewClient()
	if err != nil {
		// Handle error
	}

If your Opensearch server is running on a different IP and/or port,
just provide a URL to NewClient:

	// Create a client and connect to http://192.168.2.10:9201
	client, err := opensearch.NewClient(opensearch.SetURL("http://192.168.2.10:9201"))
	if err != nil {
	  // Handle error
	}

You can pass many more configuration parameters to NewClient. Review the
documentation of NewClient for more information.

If no Opensearch server is available, services will fail when creating
a new request and will return ErrNoClient.

A Client provides services. The services usually come with a variety of
methods to prepare the query and a Do function to execute it against the
Opensearch REST interface and return a response. Here is an example
of the IndexExists service that checks if a given index already exists.

	exists, err := client.IndexExists("twitter").Do(context.Background())
	if err != nil {
		// Handle error
	}
	if !exists {
		// Index does not exist yet.
	}

Look up the documentation for Client to get an idea of the services provided
and what kinds of responses you get when executing the Do function of a service.
Also see the wiki on Github for more details.
*/
package opensearch
