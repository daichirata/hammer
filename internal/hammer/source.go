package hammer

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
)

type Source interface {
	String() string
	DDL(context.Context, *DDLOption) (DDL, error)
}

type DDLOption struct {
	IgnoreAlterDatabase bool
	IgnoreChangeStreams bool
	IgnoreModels        bool
	IgnoreSequences     bool
}

func NewSource(ctx context.Context, uri string) (Source, error) {
	switch Scheme(uri) {
	case "spanner":
		return NewSpannerSource(ctx, uri)
	case "file", "":
		return NewFileSource(uri)
	}
	return nil, errors.New("invalid source")
}

type SpannerSource struct {
	uri    string
	client *Client
}

func NewSpannerSource(ctx context.Context, uri string) (*SpannerSource, error) {
	client, err := NewClient(ctx, uri)
	if err != nil {
		return nil, err
	}
	return &SpannerSource{uri: uri, client: client}, nil
}

func (s *SpannerSource) String() string {
	return s.uri
}

func (s *SpannerSource) DDL(ctx context.Context, option *DDLOption) (DDL, error) {
	schema, err := s.client.GetDatabaseDDL(ctx)
	if err != nil {
		return DDL{}, err
	}
	return ParseDDL(s.uri, schema, option)
}

func (s *SpannerSource) Apply(ctx context.Context, ddl DDL) error {
	return s.client.ApplyDatabaseDDL(ctx, ddl)
}

func (s *SpannerSource) Create(ctx context.Context, ddl DDL) error {
	return s.client.CreateDatabase(ctx, ddl)
}

type FileSource struct {
	uri  string
	path string
}

func NewFileSource(uri string) (*FileSource, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse uri: %s", err)
	}
	return &FileSource{uri: uri, path: u.Path}, nil
}

func (s *FileSource) String() string {
	return s.uri
}

func (s *FileSource) DDL(_ context.Context, option *DDLOption) (DDL, error) {
	schema, err := os.ReadFile(s.path)
	if err != nil {
		return DDL{}, err
	}
	return ParseDDL(s.uri, string(schema), option)
}
