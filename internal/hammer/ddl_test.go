package hammer_test

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/cloudspannerecosystem/memefish/ast"

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

CREATE CHANGE STREAM LongerDataRetention INVALID SCHEMA ();
`,
			option:  &hammer.DDLOption{},
			want:    ``,
			wantErr: true,
		},
		{
			name: "parse change streams",
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
			want: `CREATE TABLE Users (UserID STRING(10) NOT NULL, Name STRING(10) NOT NULL) PRIMARY KEY (UserID);
CREATE CHANGE STREAM LongerDataRetention FOR ALL OPTIONS (retention_period = "36h");`,
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
			want: `CREATE TABLE Users (UserID STRING(10) NOT NULL, Name STRING(10) NOT NULL) PRIMARY KEY (UserID);`,
		},
		{
			name: "Ignore change streams with small cases",
			schema: `CREATE TABLE Users (
  UserID STRING(10) NOT NULL, -- comment
  Name   STRING(10) NOT NULL, -- comment
) PRIMARY KEY(UserID);

create change stream LongerDataRetention
  for all options (
  retention_period = '36h'
);
`,
			option: &hammer.DDLOption{
				IgnoreChangeStreams: true,
			},
			want: `CREATE TABLE Users (UserID STRING(10) NOT NULL, Name STRING(10) NOT NULL) PRIMARY KEY (UserID);`,
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

func newIdent(name string) *ast.Ident {
	return &ast.Ident{Name: name}
}

func TestAlterColumn_SQL(t *testing.T) {
	values := []struct {
		d *ast.ColumnDef
		e string
		s bool
	}{
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ScalarSchemaType{Name: ast.BoolTypeName}, NotNull: true},
			e: "ALTER TABLE test_table ALTER COLUMN test_column BOOL NOT NULL",
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ScalarSchemaType{Name: ast.Int64TypeName}},
			e: "ALTER TABLE test_table ALTER COLUMN test_column INT64",
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.SizedSchemaType{Name: ast.StringTypeName, Size: &ast.IntLiteral{Value: "36"}}},
			e: "ALTER TABLE test_table ALTER COLUMN test_column STRING(36)",
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ArraySchemaType{Item: &ast.SizedSchemaType{Name: ast.StringTypeName, Size: &ast.IntLiteral{Value: "36"}}}, NotNull: true},
			e: "ALTER TABLE test_table ALTER COLUMN test_column ARRAY<STRING(36)> NOT NULL",
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ScalarSchemaType{Name: ast.TimestampTypeName}, NotNull: true},
			e: "ALTER TABLE test_table ALTER COLUMN test_column TIMESTAMP NOT NULL",
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ScalarSchemaType{Name: ast.TimestampTypeName}, NotNull: false},
			e: "ALTER TABLE test_table ALTER COLUMN test_column TIMESTAMP",
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ScalarSchemaType{Name: ast.TimestampTypeName}, NotNull: true, Options: &ast.Options{Records: []*ast.OptionsDef{{Name: newIdent("allow_commit_timestamp"), Value: &ast.BoolLiteral{Value: true}}}}},
			e: "ALTER TABLE test_table ALTER COLUMN test_column SET OPTIONS (allow_commit_timestamp = true)",
			s: true,
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ScalarSchemaType{Name: ast.TimestampTypeName}, NotNull: true, Options: &ast.Options{Records: []*ast.OptionsDef{{Name: newIdent("allow_commit_timestamp"), Value: &ast.BoolLiteral{Value: false}}}}},
			e: "ALTER TABLE test_table ALTER COLUMN test_column SET OPTIONS (allow_commit_timestamp = null)",
			s: true,
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ScalarSchemaType{Name: ast.Int64TypeName}, DefaultExpr: &ast.ColumnDefaultExpr{Expr: &ast.IntLiteral{Value: "1"}}},
			e: "ALTER TABLE test_table ALTER COLUMN test_column INT64 DEFAULT (1)",
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ScalarSchemaType{Name: ast.Int64TypeName}, NotNull: true, DefaultExpr: &ast.ColumnDefaultExpr{Expr: &ast.IntLiteral{Value: "1"}}},
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
		d *ast.ColumnDef
		s string
	}{
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ScalarSchemaType{Name: ast.BoolTypeName}},
			s: `UPDATE test_table SET test_column = FALSE WHERE test_column IS NULL`,
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ScalarSchemaType{Name: ast.Int64TypeName}},
			s: `UPDATE test_table SET test_column = 0 WHERE test_column IS NULL`,
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ArraySchemaType{Item: &ast.ScalarSchemaType{Name: ast.Int64TypeName}}},
			s: `UPDATE test_table SET test_column = [] WHERE test_column IS NULL`,
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ScalarSchemaType{Name: ast.StringTypeName}},
			s: `UPDATE test_table SET test_column = "" WHERE test_column IS NULL`,
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ScalarSchemaType{Name: ast.BytesTypeName}},
			s: `UPDATE test_table SET test_column = B"" WHERE test_column IS NULL`,
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ArraySchemaType{Item: &ast.ScalarSchemaType{Name: ast.StringTypeName}}},
			s: `UPDATE test_table SET test_column = [] WHERE test_column IS NULL`,
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ScalarSchemaType{Name: ast.DateTypeName}},
			s: `UPDATE test_table SET test_column = DATE "0001-01-01" WHERE test_column IS NULL`,
		},
		{
			d: &ast.ColumnDef{Name: newIdent("test_column"), Type: &ast.ScalarSchemaType{Name: ast.TimestampTypeName}},
			s: `UPDATE test_table SET test_column = TIMESTAMP "0001-01-01T00:00:00Z" WHERE test_column IS NULL`,
		},
		{
			d: &ast.ColumnDef{Name: newIdent("order"), Type: &ast.ScalarSchemaType{Name: ast.Int64TypeName}},
			s: "UPDATE test_table SET `order` = 0 WHERE `order` IS NULL",
		},
		{
			d: &ast.ColumnDef{Name: newIdent("json"), Type: &ast.ScalarSchemaType{Name: ast.JSONTypeName}},
			s: `UPDATE test_table SET json = JSON "{}" WHERE json IS NULL`,
		},
		{
			d: &ast.ColumnDef{Name: newIdent("default"), Type: &ast.ScalarSchemaType{Name: ast.Int64TypeName}, DefaultExpr: &ast.ColumnDefaultExpr{Expr: &ast.IntLiteral{Value: "1"}}},
			s: "UPDATE test_table SET `default` = 1 WHERE `default` IS NULL",
		},
	}
	for i, v := range values {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			actual := hammer.Update{Table: "test_table", Def: v.d}.SQL()

			if actual != v.s {
				t.Fatalf("\ngot:\n%v\nwant:\n%v\n", actual, v.s)
			}
		})
	}
}
