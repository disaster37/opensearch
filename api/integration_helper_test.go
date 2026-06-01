//go:build integration

package api_test

import (
	"os"

	opensearch "github.com/disaster37/opensearch/v4"
	"github.com/sirupsen/logrus"
)

func newIntegrationClient() opensearch.Client {
	requiredEnv := func(name string) string {
		v := os.Getenv(name)
		if v == "" {
			panic("integration tests require the " + name + " environment variable (set in ci/dagger/main.go Test())")
		}
		return v
	}

	url := requiredEnv("OPENSEARCH_URL")
	username := requiredEnv("OPENSEARCH_USERNAME")
	password := requiredEnv("OPENSEARCH_PASSWORD")

	logger := logrus.NewEntry(logrus.StandardLogger())
	client, err := opensearch.New(&opensearch.Config{
		URL:           url,
		Username:      username,
		Password:      password,
		TLSSkipVerify: true,
	}, logger)
	if err != nil {
		panic("failed to create integration client: " + err.Error())
	}
	return client
}
