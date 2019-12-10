package main

import (
	"context"
	"net/url"
	"strings"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

type Client struct {
	databaseName string
	client       *spanner.Client
	admin        *database.DatabaseAdminClient
}

func NewClient(ctx context.Context, uri string) (*Client, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	options := []option.ClientOption{}
	if credentials := u.Query().Get("credentials"); credentials != "" {
		options = append(options, option.WithCredentialsFile(credentials))
	}
	databaseName := strings.Replace(uri, "spanner://", "", 1)
	if i := strings.Index(databaseName, "?"); i > 0 {
		databaseName = databaseName[0:i]
	}

	client, err := spanner.NewClient(ctx, databaseName, options...)
	if err != nil {
		return nil, err
	}
	admin, err := database.NewDatabaseAdminClient(ctx, options...)
	if err != nil {
		return nil, err
	}

	return &Client{
		databaseName: databaseName,
		client:       client,
		admin:        admin,
	}, nil
}

func (c *Client) GetDatabaseDDL(ctx context.Context) (string, error) {
	response, err := c.admin.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{
		Database: c.databaseName,
	})
	if err != nil {
		return "", err
	}
	return strings.Join(response.Statements, ";"), nil
}

func (c *Client) ApplyDatabaseDDL(ctx context.Context, ddls []DDL) error {
	stmts := []string{}
	for _, ddl := range ddls {
		if c.isDatabaseDDL(ddl) {
			stmts = append(stmts, ddl.SQL())
		} else {
			if len(stmts) > 0 {
				if err := c.updateDatabaseDDL(ctx, stmts); err != nil {
					return err
				}
				stmts = stmts[:0]
			}
			if err := c.update(ctx, ddl.SQL()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Client) updateDatabaseDDL(ctx context.Context, stmts []string) error {
	op, err := c.admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   c.databaseName,
		Statements: stmts,
	})
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

func (c *Client) update(ctx context.Context, stmt string) error {
	_, err := c.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.Statement{
			SQL: stmt,
		})
		return err
	})
	return err
}

func (c *Client) isDatabaseDDL(ddl DDL) bool {
	switch ddl.(type) {
	case Update:
		return false
	default:
		return true
	}
}
