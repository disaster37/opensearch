// Connect simply connects to Opensearch Service on AWS.
//
// Example
//
//	aws-connect -url=https://search-xxxxx-yyyyy.eu-central-1.es.amazonaws.com
package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/olivere/env"
	"github.com/olivere/opensearch"
	awsauth "github.com/smartystreets/go-aws-auth"

	"github.com/disaster37/opensearch/v3/aws"
)

func main() {
	var (
		accessKey = flag.String("access-key", env.String("", "AWS_ACCESS_KEY"), "Access Key ID")
		secretKey = flag.String("secret-key", env.String("", "AWS_SECRET_KEY"), "Secret access key")
		url       = flag.String("url", "http://localhost:9200", "Opensearch URL")
		sniff     = flag.Bool("sniff", false, "Enable or disable sniffing")
	)
	flag.Parse()
	log.SetFlags(0)

	if *url == "" {
		*url = "http://127.0.0.1:9200"
	}
	if *accessKey == "" {
		log.Fatal("missing -access-key or AWS_ACCESS_KEY environment variable")
	}
	if *secretKey == "" {
		log.Fatal("missing -secret-key or AWS_SECRET_KEY environment variable")
	}

	signingClient := aws.NewV4SigningClient(awsauth.Credentials{
		AccessKeyID:     *accessKey,
		SecretAccessKey: *secretKey,
	})

	// Create an Opensearch client
	client, err := opensearch.NewClient(
		opensearch.SetURL(*url),
		opensearch.SetSniff(*sniff),
		opensearch.SetHealthcheck(*sniff),
		opensearch.SetHttpClient(signingClient),
	)
	if err != nil {
		log.Fatal(err)
	}
	_ = client

	// Just a status message
	fmt.Println("Connection succeeded")
}
