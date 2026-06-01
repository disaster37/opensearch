package api

import (
	"context"
	"fmt"
	json "github.com/goccy/go-json"
	"strings"

	"github.com/disaster37/opensearch/v4/types"
	"github.com/go-resty/resty/v2"
	"github.com/sirupsen/logrus"
)

type SnapshotService interface {
	Create(ctx context.Context, req *SnapshotCreateRequest) (*SnapshotCreateResponse, error)
	Get(ctx context.Context, req *SnapshotGetRequest) (*SnapshotGetResponse, error)
	Delete(ctx context.Context, req *SnapshotDeleteRequest) (*SnapshotDeleteResponse, error)
	Status(ctx context.Context, req *SnapshotStatusRequest) (*SnapshotStatusResponse, error)
	Restore(ctx context.Context, req *SnapshotRestoreRequest) (*SnapshotRestoreResponse, error)
	CreateRepository(ctx context.Context, repository string, body any) (*SnapshotCreateRepositoryResponse, error)
	GetRepository(ctx context.Context, repositories []string) (map[string]*SnapshotGetRepositoryResponse, error)
	DeleteRepository(ctx context.Context, repository string) (*SnapshotDeleteRepositoryResponse, error)
	VerifyRepository(ctx context.Context, repository string) (*SnapshotVerifyRepositoryResponse, error)
	CleanupRepository(ctx context.Context, repository string) (*SnapshotCleanupRepositoryResponse, error)
	Clone(ctx context.Context, req *SnapshotCloneRequest) (*types.AcknowledgedResponse, error)
}

type DefaultSnapshotService struct {
	client *resty.Client
	logger *logrus.Entry
}

func NewSnapshotService(client *resty.Client, logger *logrus.Entry) SnapshotService {
	return &DefaultSnapshotService{client: client, logger: logger.WithField("service", "snapshot")}
}

func (s *DefaultSnapshotService) Create(ctx context.Context, req *SnapshotCreateRequest) (*SnapshotCreateResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/_snapshot/%s/%s", req.Repository, req.Snapshot)
	r := s.client.R().SetContext(ctx)
	if req.Body != nil {
		r.SetBody(req.Body)
	}

	resp, err := r.Put(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SnapshotCreateResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSnapshotService) Get(ctx context.Context, req *SnapshotGetRequest) (*SnapshotGetResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/_snapshot/%s/_all", req.Repository)
	if len(req.Snapshots) > 0 {
		path = fmt.Sprintf("/_snapshot/%s/%s", req.Repository, strings.Join(req.Snapshots, ","))
	}

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SnapshotGetResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSnapshotService) Delete(ctx context.Context, req *SnapshotDeleteRequest) (*SnapshotDeleteResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/_snapshot/%s/%s", req.Repository, req.Snapshot)

	resp, err := s.client.R().SetContext(ctx).Delete(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SnapshotDeleteResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSnapshotService) Status(ctx context.Context, req *SnapshotStatusRequest) (*SnapshotStatusResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/_snapshot/%s/_status", req.Repository)
	if len(req.Snapshots) > 0 {
		path = fmt.Sprintf("/_snapshot/%s/%s/_status", req.Repository, strings.Join(req.Snapshots, ","))
	}

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SnapshotStatusResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSnapshotService) Restore(ctx context.Context, req *SnapshotRestoreRequest) (*SnapshotRestoreResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/_snapshot/%s/%s/_restore", req.Repository, req.Snapshot)
	r := s.client.R().SetContext(ctx)
	if req.Body != nil {
		r.SetBody(req.Body)
	}

	resp, err := r.Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SnapshotRestoreResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSnapshotService) CreateRepository(ctx context.Context, repository string, body any) (*SnapshotCreateRepositoryResponse, error) {
	if repository == "" {
		return nil, fmt.Errorf("repository is required")
	}
	if body == nil {
		return nil, fmt.Errorf("body is required")
	}

	path := fmt.Sprintf("/_snapshot/%s", repository)

	resp, err := s.client.R().SetContext(ctx).SetBody(body).Put(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SnapshotCreateRepositoryResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSnapshotService) GetRepository(ctx context.Context, repositories []string) (map[string]*SnapshotGetRepositoryResponse, error) {
	path := "/_snapshot"
	if len(repositories) > 0 {
		path = fmt.Sprintf("/_snapshot/%s", strings.Join(repositories, ","))
	}

	resp, err := s.client.R().SetContext(ctx).Get(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result map[string]*SnapshotGetRepositoryResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return result, nil
}

func (s *DefaultSnapshotService) DeleteRepository(ctx context.Context, repository string) (*SnapshotDeleteRepositoryResponse, error) {
	if repository == "" {
		return nil, fmt.Errorf("repository is required")
	}

	path := fmt.Sprintf("/_snapshot/%s", repository)

	resp, err := s.client.R().SetContext(ctx).Delete(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SnapshotDeleteRepositoryResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSnapshotService) VerifyRepository(ctx context.Context, repository string) (*SnapshotVerifyRepositoryResponse, error) {
	if repository == "" {
		return nil, fmt.Errorf("repository is required")
	}

	path := fmt.Sprintf("/_snapshot/%s/_verify", repository)

	resp, err := s.client.R().SetContext(ctx).Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SnapshotVerifyRepositoryResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSnapshotService) CleanupRepository(ctx context.Context, repository string) (*SnapshotCleanupRepositoryResponse, error) {
	if repository == "" {
		return nil, fmt.Errorf("repository is required")
	}

	path := fmt.Sprintf("/_snapshot/%s/_cleanup", repository)

	resp, err := s.client.R().SetContext(ctx).Post(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result SnapshotCleanupRepositoryResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}

func (s *DefaultSnapshotService) Clone(ctx context.Context, req *SnapshotCloneRequest) (*types.AcknowledgedResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	path := fmt.Sprintf("/_snapshot/%s/%s/_clone/%s", req.Repository, req.Snapshot, req.TargetSnapshot)
	r := s.client.R().SetContext(ctx)
	if req.Body != nil {
		r.SetBody(req.Body)
	}

	resp, err := r.Put(path)
	if err != nil {
		return nil, wrapNetworkError(s.logger, err)
	}
	if resp.IsError() {
		return nil, logAndReturnError(s.logger, resp)
	}

	var result types.AcknowledgedResponse
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, wrapUnmarshalError(s.logger, resp, err)
	}
	return &result, nil
}
