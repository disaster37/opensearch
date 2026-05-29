// Connect simply connects to Opensearch.
//
// # Example
//
//	connect "http://127.0.0.1:9200/test-index?sniff=false&healthcheck=false"
package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"

	"github.com/disaster37/opensearch/v3/config"
	"github.com/olivere/opensearch"
)

func main() {
	flag.Parse()
	log.SetFlags(0)

	url := flag.Arg(0)
	if url == "" {
		url = "http://127.0.0.1:9200"
	}

	// Parse the URL with the config package
	cfg, err := config.Parse(url)
	if err != nil {
		log.Fatal(err)
	}

	// Create an Opensearch client
	client, err := opensearch.NewClientFromConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Get ES version
	esversion, err := client.OpensearchVersion(cfg.URL)
	if err != nil {
		log.Fatal(err)
	}

	// Just a status message
	fmt.Printf(
		"Connection succeeded with %v, Elastic %v and Opensearch %s\n",
		runtime.Version(),
		opensearch.Version,
		esversion,
	)
}
