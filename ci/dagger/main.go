// A generated module for Opensearch functions
//
// This module has been generated via dagger init and serves as a reference to
// basic module structure as you get started with Dagger.
//
// Two functions have been pre-created. You can modify, delete, or add to them,
// as needed. They demonstrate usage of arguments and return types using simple
// echo and grep commands. The functions can be called from the dagger CLI or
// from one of the SDKs.
//
// The first line in this comment block is a short description line and the
// rest is a long description with more detail on the module's purpose or usage,
// if appropriate. All modules should have a short description.

package main

import (
	"context"
	"dagger/opensearch/internal/dagger"
	"fmt"

	"emperror.dev/errors"
	"github.com/disaster37/dagger-library-go/lib/helper"
)

const (
	OpensearchVersion string = "2.18.0"
	username          string = "admin"
	password          string = "vLPeJYa8.3RqtZCcAK6jNz"
	mockgenVersion           = "v0.3.0"
)

type Opensearch struct {
	// Src is a directory that contains the projects source code
	// +private
	Src *dagger.Directory

	// The golang base image
	// +private
	BaseImage *dagger.Container

	// +private
	GolangModule *dagger.Golang
}

func New(
	ctx context.Context,
	// a path to a directory containing the source code
	// +required
	src *dagger.Directory,
) (*Opensearch, error) {

	// Compute image because of base is not optional
	version, err := inspectModVersion(context.Background(), src)
	if err != nil {
		return nil, err
	}
	base := defaultImage(version)
	base = mountCaches(ctx, base).
		WithDirectory(goWorkDir, src).
		WithWorkdir(goWorkDir).
		WithoutEntrypoint()

	return &Opensearch{
		Src:          src,
		GolangModule: dag.Golang(base, src),
		BaseImage:    base,
	}, nil
}

func (h *Opensearch) Ci(
	ctx context.Context,

	// Set tru if you are on CI
	// +default=false
	ci bool,

	// The codeCov token
	// +optional
	codeCoveToken *dagger.Secret,

	// The git token
	// +optional
	gitToken *dagger.Secret,
) (dir *dagger.Directory, err error) {
	var stdout string

	// Build
	h.Build(ctx)

	// Lint code
	stdout, err = h.Lint(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Error when lint project: %s", stdout)
	}

	// Format code
	dir = h.Format(ctx)

	// Test code
	reportFile := h.Test(ctx, "")
	dir = dir.WithFile("coverage.out", reportFile)

	if ci {
		if codeCoveToken == nil {
			return nil, errors.New("You need to provide CodeCov token")
		}
		stdout, err = h.CodeCov(ctx, dir, codeCoveToken)
		if err != nil {
			return nil, errors.Wrapf(err, "Error when upload report on CodeCov: %s", stdout)
		}
	}

	return dir, nil

}

// Lint permit to lint code
func (h *Opensearch) Lint(
	ctx context.Context,
) (string, error) {
	return h.GolangModule.Lint(ctx)
}

// Format permit to format the golang code
func (h *Opensearch) Format(
	ctx context.Context,
) *dagger.Directory {
	return h.GolangModule.Format()
}

// Test permit to run tests
func (h *Opensearch) Test(
	ctx context.Context,
	//run select tests only, defined using a regex

	// +optional
	run string,
) *dagger.File {

	expectedRunTest := ""
	if run != "" {
		expectedRunTest = fmt.Sprintf("-run %s", run)
	}

	// Run Opensearch
	opensearchService := dag.Container().
		From(fmt.Sprintf("opensearchproject/opensearch:%s", OpensearchVersion)).
		WithEnvVariable("cluster.name", "test").
		WithEnvVariable("node.name", "opensearch-node1").
		WithEnvVariable("bootstrap.memory_lock", "true").
		WithEnvVariable("discovery.type", "single-node").
		WithEnvVariable("network.publish_host", "0.0.0.0").
		WithEnvVariable("logger.org.opensearchsearch", "warn").
		WithEnvVariable("OPENSEARCH_JAVA_OPTS", "-Xms1g -Xmx1g").
		WithEnvVariable("plugins.security.nodes_dn_dynamic_config_enabled", "true").
		WithEnvVariable("plugins.security.unsupported.restapi.allow_securityconfig_modification", "true").
		WithEnvVariable("OPENSEARCH_INITIAL_ADMIN_PASSWORD", password).
		WithEnvVariable("path.repo", "/usr/share/opensearch/backup").
		WithExposedPort(9200).
		AsService()

	return h.BaseImage.
		WithServiceBinding("opensearch.svc", opensearchService).
		WithExposedPort(4000).
		WithExec(helper.ForgeScript(`
curl --fail -XGET -k -u admin:vLPeJYa8.3RqtZCcAK6jNz "https://opensearch.svc:9200/_cluster/health?wait_for_status=yellow&timeout=500s"
curl --fail -XPUT -k -u admin:vLPeJYa8.3RqtZCcAK6jNz -H 'Content-Type: application/json' "https://opensearch.svc:9200/_index_template/socle" -d '{"index_patterns":["*"],"priority":500,"template":{"settings":{"number_of_shards":1,"number_of_replicas":0}}}'
go install gotest.tools/gotestsum@latest
gotestsum --format testname -- -covermode=atomic -coverprofile coverage.out ./... %s
		`, expectedRunTest)).
		File("coverage.out")
}

// Test permit to run tests
func (h *Opensearch) DebugTest(
	ctx context.Context,
	//run select tests only, defined using a regex

	// +optional
	run string,
) *dagger.Service {

	expectedRunTest := ""
	if run != "" {
		expectedRunTest = fmt.Sprintf("-- -test.run %s", run)
	}

	// Run Opensearch
	opensearchService := dag.Container().
		From(fmt.Sprintf("opensearchproject/opensearch:%s", OpensearchVersion)).
		WithEnvVariable("cluster.name", "test").
		WithEnvVariable("node.name", "opensearch-node1").
		WithEnvVariable("bootstrap.memory_lock", "true").
		WithEnvVariable("discovery.type", "single-node").
		WithEnvVariable("network.publish_host", "0.0.0.0").
		WithEnvVariable("logger.org.opensearchsearch", "warn").
		WithEnvVariable("OPENSEARCH_JAVA_OPTS", "-Xms1g -Xmx1g").
		WithEnvVariable("plugins.security.nodes_dn_dynamic_config_enabled", "true").
		WithEnvVariable("plugins.security.unsupported.restapi.allow_securityconfig_modification", "true").
		WithEnvVariable("OPENSEARCH_INITIAL_ADMIN_PASSWORD", password).
		WithEnvVariable("path.repo", "/usr/share/opensearch/backup").
		WithExposedPort(9200).
		AsService()

	return h.BaseImage.
		WithServiceBinding("opensearch.svc", opensearchService).
		WithExposedPort(4000).
		WithExec(helper.ForgeScript(`
curl --fail -XGET -k -u admin:vLPeJYa8.3RqtZCcAK6jNz "https://opensearch.svc:9200/_cluster/health?wait_for_status=yellow&timeout=500s"
curl --fail -XPUT -k -u admin:vLPeJYa8.3RqtZCcAK6jNz -H 'Content-Type: application/json' "https://opensearch.svc:9200/_index_template/socle" -d '{"index_patterns":["*"],"priority":500,"template":{"settings":{"number_of_shards":1,"number_of_replicas":0}}}'
go install github.com/acroca/go-symbols@latest &&\
go install github.com/cweill/gotests/gotests@latest &&\
go install github.com/davidrjenni/reftools/cmd/fillstruct@latest &&\
go install github.com/haya14busa/goplay/cmd/goplay@latest &&\
go install github.com/stamblerre/gocode@latest &&\
mv /go/bin/gocode /go/bin/gocode-gomod &&\
go install github.com/mdempsky/gocode@latest &&\
go install github.com/ramya-rao-a/go-outline@latest &&\
go install github.com/rogpeppe/godef@latest &&\
go install github.com/sqs/goreturns@latest &&\
go install github.com/uudashr/gopkgs/v2/cmd/gopkgs@latest &&\
go install github.com/zmb3/gogetdoc@latest &&\
go install honnef.co/go/tools/cmd/staticcheck@latest &&\
go install golang.org/x/tools/cmd/gorename@latest &&\
go install github.com/go-delve/delve/cmd/dlv@latest &&\
go install golang.org/x/tools/gopls@latest
dlv test --listen=:4000 --log=true --headless=true --accept-multiclient --api-version=2 %s
		`, expectedRunTest)).
		AsService()
}

// Build permit to build project
func (h *Opensearch) Build(
	ctx context.Context,
) *dagger.Directory {
	return h.GolangModule.Build()
}

func (h *Opensearch) CodeCov(
	ctx context.Context,

	// Optional directory
	// +optional
	src *dagger.Directory,

	// The Codecov token
	// +required
	token *dagger.Secret,
) (stdout string, err error) {

	if src == nil {
		src = h.Src
	}

	return dag.Codecov().Upload(
		ctx,
		src,
		token,
		dagger.CodecovUploadOpts{
			Files:   []string{"coverage.out"},
			Verbose: true,
		},
	)
}
