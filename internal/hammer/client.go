package hammer

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1"
	"google.golang.org/api/option"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"google.golang.org/grpc"
)

type Client struct {
	database string
	client   *spanner.Client
	admin    *database.DatabaseAdminClient
}

func NewClient(ctx context.Context, uri string) (*Client, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	db := u.Host + u.Path

	opts := []option.ClientOption{}
	if credentials := u.Query().Get("credentials"); credentials != "" {
		opts = append(opts, option.WithCredentialsFile(credentials))
	}

	if host, ok := os.LookupEnv("SPANNER_EMULATOR_HOST"); ok {
		dialCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		conn, err := grpc.DialContext(dialCtx, host, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		opts =  append(opts, option.WithGRPCConn(conn))
	}

	client, err := spanner.NewClient(ctx, db, opts...)
	if err != nil {
		return nil, err
	}
	admin, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &Client{
		database: db,
		client:   client,
		admin:    admin,
	}, nil
}

func (c *Client) GetDatabaseDDL(ctx context.Context) (string, error) {
	response, err := c.admin.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{
		Database: c.database,
	})
	if err != nil {
		return "", err
	}
	return strings.Join(response.Statements, ";\n"), nil
}

func (c *Client) CreateDatabase(ctx context.Context, ddl DDL) error {
	parts := strings.Split(c.database, "/")
	stmts := make([]string, len(ddl.List))
	for i, stmt := range ddl.List {
		stmts[i] = stmt.SQL()
	}
	op, err := c.admin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", parts[1], parts[3]),
		CreateStatement: fmt.Sprintf("CREATE DATABASE %s", parts[5]),
		ExtraStatements: stmts,
	})
	if err != nil {
		return err
	}
	_, err = op.Wait(ctx)
	return err
}

func (c *Client) ApplyDatabaseDDL(ctx context.Context, ddl DDL) error {
	var stmts []string
	for _, stmt := range ddl.List {
		if c.isUpdateDatabaseStatement(stmt) {
			stmts = append(stmts, stmt.SQL())
		} else {
			if len(stmts) > 0 {
				if err := c.updateDatabaseDDL(ctx, stmts); err != nil {
					return err
				}
				stmts = stmts[:0]
			}
			if err := c.partitionedUpdate(ctx, stmt.SQL()); err != nil {
				return err
			}
		}
	}
	if len(stmts) > 0 {
		if err := c.updateDatabaseDDL(ctx, stmts); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) updateDatabaseDDL(ctx context.Context, stmts []string) error {
	op, err := c.admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   c.database,
		Statements: stmts,
	})
	if err != nil {
		return err
	}
	return op.Wait(ctx)
}

func (c *Client) partitionedUpdate(ctx context.Context, stmt string) error {
	_, err := c.client.PartitionedUpdate(ctx, spanner.Statement{SQL: stmt})
	return err
}

func (c *Client) isUpdateDatabaseStatement(stmt Statement) bool {
	switch stmt.(type) {
	case Update:
		return false
	default:
		return true
	}
}
