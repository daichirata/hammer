package hammer_test

import (
	"testing"

	"cloud.google.com/go/spanner/spansql"

	"github.com/daichirata/hammer/internal/hammer"
)

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
	}
	for _, v := range values {
		actual := hammer.Update{Table: "test_table", Def: v.d}.SQL()

		if actual != v.s {
			t.Fatalf("got: %v, want: %v", actual, v.s)
		}
	}
}
