package main

import (
	"context"
	"io/ioutil"
	"strings"

	"google.golang.org/api/option"
	"google.golang.org/api/spanner/v1"
)

type SpannerSource struct {
	path    string
	service *spanner.ProjectsInstancesDatabasesService
}

func NewSpannerSource(ctx context.Context, path string, opts ...option.ClientOption) (*SpannerSource, error) {
	spannerService, err := spanner.NewService(ctx, opts...)
	if err != nil {
		return nil, err
	}
	service := spanner.NewProjectsInstancesDatabasesService(spannerService)

	return &SpannerSource{path: path, service: service}, nil
}

func NewSpannerSourceWithCredentials(ctx context.Context, path, credentials string) (*SpannerSource, error) {
	return NewSpannerSource(ctx, path, option.WithCredentialsFile(credentials))
}

func (s *SpannerSource) Read() (string, error) {
	response, err := s.service.GetDdl(s.path).Do()
	if err != nil {
		return "", err
	}
	return strings.Join(response.Statements, ";"), nil
}

type FileSource struct {
	path string
}

func NewFileSource(path string) *FileSource {
	return &FileSource{path: path}
}

func (s *FileSource) Read() (string, error) {
	ddls, err := ioutil.ReadFile(s.path)
	if err != nil {
		return "", err
	}
	return string(ddls), nil
}
