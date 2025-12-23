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
	"fmt"
	"time"

	"dagger/opensearch/internal/dagger"

	"emperror.dev/errors"
	"github.com/disaster37/dagger-library-go/lib/helper"
)

const (
	OpensearchVersion string = "3.4.0"
	username          string = "admin"
	password          string = "vLPeJYa8.3RqtZCcAK6jNz"
	gitUsername       string = "ci"
	gitEmail          string = "ci@localhost"
	defaultGitBranch  string = "release-branch.v3"
)

type Opensearch struct {
	// Src is a directory that contains the projects source code
	// +private
	Src *dagger.Directory

	// +private
	GolangModule *dagger.Golang
}

func New(
	ctx context.Context,
	// a path to a directory containing the source code
	// +required
	src *dagger.Directory,
) (*Opensearch, error) {

	return &Opensearch{
		Src:          src,
		GolangModule: dag.Golang(src.WithoutDirectory("ci")),
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

	// The git branch where you should to push
	// You need to provide it when you are on PullRequest or on Tag
	// +optional
	gitBranch string,

	// Set true if current build is a tag
	// It will use the stable and alpha channel
	// alpha channel only instead
	// +optional
	isTag bool,

	// The git token
	// +optional
	gitToken *dagger.Secret,
) (dir *dagger.Directory, err error) {
	var stdout string

	h.GolangModule = dag.Golang(h.Src.WithoutDirectory("ci"), dagger.GolangOpts{Base: h.GolangModule.Container().WithExec([]string{"go", "mod", "tidy"})})

	// Build
	if _, err = h.Build(ctx).Sync(ctx); err != nil {
		return nil, errors.Wrap(err, "Error when build project")
	}

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

		git := dag.GitModule(dir.WithDirectory("ci", h.Src.Directory("ci")), dagger.GitModuleOpts{Ci: "github"}).
			SetConfig(dagger.GitModuleSetConfigOpts{
				Username: gitUsername,
				Email:    gitEmail,
			})

		if isTag {
			gitBranch = defaultGitBranch
		}

		if _, err = git.CommitAndPush(
			ctx,
			gitToken,
			dagger.GitModuleCommitAndPushOpts{
				BranchName: gitBranch,
				GitRepoURL: "https://github.com/disaster37/opensearch.git",
				Message:    "Commit from CI",
			},
		); err != nil {
			return nil, errors.Wrap(err, "Error when commit and push files change")
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

func (h *Opensearch) Opensearch(ctx context.Context) *dagger.Service {

	opensearchLeaderService := dag.Container().
		From(fmt.Sprintf("opensearchproject/opensearch:%s", OpensearchVersion)).
		WithEnvVariable("cluster.name", "leader").
		WithEnvVariable("node.name", "opensearch-leader-node1").
		WithEnvVariable("node.roles", "remote_cluster_client, ingest, data, cluster_manager").
		WithEnvVariable("bootstrap.memory_lock", "true").
		WithEnvVariable("discovery.type", "single-node").
		WithEnvVariable("network.publish_host", "0.0.0.0").
		WithEnvVariable("logger.org.opensearchsearch", "warn").
		WithEnvVariable("OPENSEARCH_JAVA_OPTS", "-Xms512M -Xmx512M").
		WithEnvVariable("plugins.security.nodes_dn_dynamic_config_enabled", "true").
		WithEnvVariable("OPENSEARCH_INITIAL_ADMIN_PASSWORD", password).
		WithEnvVariable("path.repo", "/usr/share/opensearch/backup").
		WithEnvVariable("CLEAN_CACHE", time.Now().String()).
		WithExposedPort(9200).
		WithExposedPort(9300).
		AsService()

	stdout, err := dag.Container().
		From("alpine/curl").
		WithEntrypoint([]string{"sh", "-c"}).
		WithServiceBinding("opensearch-leader.svc", opensearchLeaderService).
		WithEnvVariable("CLEAN_CACHE", time.Now().String()).
		WithExec(helper.ForgeScript(`
sleep 10
curl --fail -XGET -k -u admin:vLPeJYa8.3RqtZCcAK6jNz https://opensearch-leader.svc:9200/_cluster/health?wait_for_status=yellow&timeout=60s
curl --fail -XPUT -k -u admin:vLPeJYa8.3RqtZCcAK6jNz -H 'Content-Type: application/json' https://opensearch-leader.svc:9200/leader-01 -d '{"settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}}}'
curl --fail -XGET -k -u admin:vLPeJYa8.3RqtZCcAK6jNz -H 'Content-Type: application/json' https://opensearch-leader.svc:9200/leader-01
	`)).
		WithExec([]string{"sh", "-c", "echo done"}).
		Stdout(ctx)

	if err != nil {
		panic(err)
	}

	opensearchFollowerService := dag.Container().
		From(fmt.Sprintf("opensearchproject/opensearch:%s", OpensearchVersion)).
		WithEnvVariable("cluster.name", "test").
		WithEnvVariable("node.roles", "remote_cluster_client, ingest, data, cluster_manager").
		WithEnvVariable("node.name", "opensearch-node1").
		WithEnvVariable("bootstrap.memory_lock", "true").
		WithEnvVariable("discovery.type", "single-node").
		WithEnvVariable("network.publish_host", "0.0.0.0").
		WithEnvVariable("logger.org.opensearchsearch", "warn").
		WithEnvVariable("OPENSEARCH_JAVA_OPTS", "-Xms1g -Xmx1g").
		WithEnvVariable("plugins.security.nodes_dn_dynamic_config_enabled", "true").
		WithEnvVariable("OPENSEARCH_INITIAL_ADMIN_PASSWORD", password).
		WithEnvVariable("path.repo", "/usr/share/opensearch/backup").
		WithEnvVariable("CLEAN_CACHE", time.Now().String()).
		WithEnvVariable("FORCE_WAIT", stdout).
		WithExposedPort(9200).
		WithServiceBinding("opensearch-leader.svc", opensearchLeaderService).
		AsService()

	if _, err := dag.Container().
		From("alpine/curl").
		WithEntrypoint([]string{"sh", "-c"}).
		WithServiceBinding("opensearch.svc", opensearchFollowerService).
		WithEnvVariable("CLEAN_CACHE", time.Now().String()).
		WithExec(helper.ForgeScript(`
sleep 10
curl --fail -XGET -k -u admin:vLPeJYa8.3RqtZCcAK6jNz https://opensearch.svc:9200/_cluster/health?wait_for_status=yellow&timeout=60s
curl -XPUT -k -H 'Content-Type: application/json' -u admin:vLPeJYa8.3RqtZCcAK6jNz 'https://opensearch.svc:9200/_cluster/settings?pretty' -d '{"persistent":{"cluster":{"remote":{"test":{"seeds":["opensearch-leader.svc:9300"]}}}}}'
	`)).
		Stdout(ctx); err != nil {
		panic(err)
	}

	return opensearchFollowerService
}

// Test permit to run tests
func (h *Opensearch) Test(
	ctx context.Context,
	// run select tests only, defined using a regex

	// +optional
	run string,
) *dagger.File {
	expectedRunTest := ""
	if run != "" {
		expectedRunTest = fmt.Sprintf("-run %s", run)
	}

	// Run Opensearch
	opensearchService := h.Opensearch(ctx)

	return h.GolangModule.Container().
		WithServiceBinding("opensearch.svc", opensearchService).
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
	// run select tests only, defined using a regex

	// +optional
	run string,
) *dagger.Service {
	expectedRunTest := ""
	if run != "" {
		expectedRunTest = fmt.Sprintf("-- -test.run %s", run)
	}

	// Run Opensearch
	opensearchService := h.Opensearch(ctx)
	defer opensearchService.Stop(ctx)

	return h.GolangModule.Container().
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
			Files: []string{"coverage.out"},
		},
	)
}
