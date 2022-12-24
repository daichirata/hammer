package hammer_test

import (
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/spanner/spansql"

	"github.com/daichirata/hammer/internal/hammer"
)

func TestParseDDL(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		option  *hammer.DDLOption
		want    string
		wantErr bool
	}{
		{
			name: "Failed to parse change streams",
			schema: `CREATE TABLE Users (
  UserID STRING(10) NOT NULL, -- comment
  Name   STRING(10) NOT NULL, -- comment
) PRIMARY KEY(UserID);

CREATE CHANGE STREAM LongerDataRetention
  FOR ALL OPTIONS (
  retention_period = '36h'
);
`,
			option: &hammer.DDLOption{},
			want: `CREATE TABLE Users (
  UserID STRING(10) NOT NULL,
  Name STRING(10) NOT NULL,
) PRIMARY KEY(UserID);
CREATE CHANGE STREAM LongerDataRetention FOR ALL OPTIONS( retention_period='36h' );`,
		},
		{
			name: "Ignore change streams",
			schema: `CREATE TABLE Users (
  UserID STRING(10) NOT NULL, -- comment
  Name   STRING(10) NOT NULL, -- comment
) PRIMARY KEY(UserID);

CREATE CHANGE STREAM LongerDataRetention
  FOR ALL OPTIONS (
  retention_period = '36h'
);
`,
			option: &hammer.DDLOption{
				IgnoreChangeStreams: true,
			},
			want: `CREATE TABLE Users (
  UserID STRING(10) NOT NULL,
  Name STRING(10) NOT NULL,
) PRIMARY KEY(UserID);`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantErr {
				if _, err := hammer.ParseDDL("", tt.schema, tt.option); err == nil {
					t.Fatalf("got nil want error")
				}
			} else {
				ddl, err := hammer.ParseDDL("", tt.schema, tt.option)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				actual := strings.Join(convertStrings(ddl), ";\n") + ";"

				if !reflect.DeepEqual(actual, tt.want) {
					t.Fatalf("\ngot:\n%s\nwant:\n%s\n", actual, tt.want)
				}
			}
		})
	}
}

func TestAlterColumn_SQL(t *testing.T) {
	values := []struct {
		d spansql.ColumnDef
		e string
		s bool
	}{
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Bool}, NotNull: true},
			e: "ALTER TABLE test_table ALTER COLUMN test_column BOOL NOT NULL",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Int64}},
			e: "ALTER TABLE test_table ALTER COLUMN test_column INT64",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.String, Len: 36}},
			e: "ALTER TABLE test_table ALTER COLUMN test_column STRING(36)",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.String, Len: 36, Array: true}, NotNull: true},
			e: "ALTER TABLE test_table ALTER COLUMN test_column ARRAY<STRING(36)> NOT NULL",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Timestamp}, NotNull: true},
			e: "ALTER TABLE test_table ALTER COLUMN test_column TIMESTAMP NOT NULL",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Timestamp}, NotNull: false},
			e: "ALTER TABLE test_table ALTER COLUMN test_column TIMESTAMP",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Timestamp}, NotNull: true, Options: spansql.ColumnOptions{AllowCommitTimestamp: &[]bool{true}[0]}},
			e: "ALTER TABLE test_table ALTER COLUMN test_column SET OPTIONS (allow_commit_timestamp = true)",
			s: true,
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Timestamp}, NotNull: true, Options: spansql.ColumnOptions{AllowCommitTimestamp: &[]bool{false}[0]}},
			e: "ALTER TABLE test_table ALTER COLUMN test_column SET OPTIONS (allow_commit_timestamp = null)",
			s: true,
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Int64}, Default: spansql.IntegerLiteral(1)},
			e: "ALTER TABLE test_table ALTER COLUMN test_column INT64 DEFAULT (1)",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Int64}, NotNull: true, Default: spansql.IntegerLiteral(1)},
			e: "ALTER TABLE test_table ALTER COLUMN test_column INT64 NOT NULL DEFAULT (1)",
		},
	}
	for _, v := range values {
		actual := hammer.AlterColumn{Table: "test_table", Def: v.d, SetOptions: v.s}.SQL()

		if actual != v.e {
			t.Fatalf("got: %v, want: %v", actual, v.e)
		}
	}
}

func TestUpdate_SQL(t *testing.T) {
	values := []struct {
		d spansql.ColumnDef
		s string
	}{
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Bool}},
			s: "UPDATE test_table SET test_column = false WHERE test_column IS NULL",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Int64}},
			s: "UPDATE test_table SET test_column = 0 WHERE test_column IS NULL",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Int64, Array: true}},
			s: "UPDATE test_table SET test_column = [] WHERE test_column IS NULL",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.String}},
			s: "UPDATE test_table SET test_column = '' WHERE test_column IS NULL",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Bytes}},
			s: "UPDATE test_table SET test_column = b'' WHERE test_column IS NULL",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.String, Array: true}},
			s: "UPDATE test_table SET test_column = [] WHERE test_column IS NULL",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Date}},
			s: "UPDATE test_table SET test_column = '0001-01-01' WHERE test_column IS NULL",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Timestamp}},
			s: "UPDATE test_table SET test_column = '0001-01-01T00:00:00Z' WHERE test_column IS NULL",
		},
		{
			d: spansql.ColumnDef{Name: "order", Type: spansql.Type{Base: spansql.Int64}},
			s: "UPDATE test_table SET `order` = 0 WHERE `order` IS NULL",
		},
		{
			d: spansql.ColumnDef{Name: "json", Type: spansql.Type{Base: spansql.JSON}},
			s: "UPDATE test_table SET json = JSON '{}' WHERE json IS NULL",
		},
		{
			d: spansql.ColumnDef{Name: "default", Type: spansql.Type{Base: spansql.Int64}, Default: spansql.IntegerLiteral(1)},
			s: "UPDATE test_table SET `default` = 1 WHERE `default` IS NULL",
		},
	}
	for _, v := range values {
		actual := hammer.Update{Table: "test_table", Def: v.d}.SQL()

		if actual != v.s {
			t.Fatalf("got: %v, want: %v", actual, v.s)
		}
	}
}
