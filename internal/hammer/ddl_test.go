package hammer_test

import (
	"testing"

	"cloud.google.com/go/spanner/spansql"

	"github.com/daichirata/hammer/internal/hammer"
)

func TestAlterColumn_SQL(t *testing.T) {
	values := []struct {
		d spansql.ColumnDef
		s string
	}{
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Bool}, NotNull: true},
			s: "ALTER TABLE test_table ALTER COLUMN test_column BOOL NOT NULL",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.Int64}},
			s: "ALTER TABLE test_table ALTER COLUMN test_column INT64",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.String, Len: 36}},
			s: "ALTER TABLE test_table ALTER COLUMN test_column STRING(36)",
		},
		{
			d: spansql.ColumnDef{Name: "test_column", Type: spansql.Type{Base: spansql.String, Len: 36, Array: true}, NotNull: true},
			s: "ALTER TABLE test_table ALTER COLUMN test_column ARRAY<STRING(36)> NOT NULL",
		},
	}
	for _, v := range values {
		actual := hammer.AlterColumn{Table: "test_table", Def: v.d}.SQL()

		if actual != v.s {
			t.Fatalf("got: %v, want: %v", actual, v.s)
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
			s: "UPDATE test_table SET test_column = CAST('' AS BYTES) WHERE test_column IS NULL",
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
			s: "UPDATE test_table SET test_column = '0001-01-01 00:00:00' WHERE test_column IS NULL",
		},
	}
	for _, v := range values {
		actual := hammer.Update{Table: "test_table", Def: v.d}.SQL()

		if actual != v.s {
			t.Fatalf("got: %v, want: %v", actual, v.s)
		}
	}
}
