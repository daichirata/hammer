package hammer

import (
	"fmt"

	"cloud.google.com/go/spanner/spansql"
)

type Statement interface {
	SQL() string
}

type DDL struct {
	List []Statement
}

func (d *DDL) Append(stmts ...Statement) {
	d.List = append(d.List, stmts...)
}

func (d *DDL) AppendDDL(ddl DDL) {
	d.Append(ddl.List...)
}

func ParseDDL(uri, schema string) (DDL, error) {
	ddl, err := spansql.ParseDDL(schema)
	if err != nil {
		return DDL{}, fmt.Errorf("%s failed to parse ddl: %w", uri, err)
	}
	list := make([]Statement, len(ddl.List))
	for i, stmt := range ddl.List {
		list[i] = stmt
	}
	return DDL{List: list}, nil
}

type AlterColumn struct {
	Table string
	Def   spansql.ColumnDef
}

func (a AlterColumn) SQL() string {
	return "ALTER TABLE " + a.Table + " ALTER COLUMN " + a.Def.SQL()
}

type Update struct {
	Table string
	Def   spansql.ColumnDef
}

func (u Update) defaultValue() string {
	if u.Def.Type.Array {
		return "[]"
	}
	switch u.Def.Type.Base {
	case spansql.Bool:
		return "false"
	case spansql.Int64, spansql.Float64:
		return "0"
	case spansql.String, spansql.Bytes:
		return "''"
	case spansql.Date:
		return "'0001-01-01'"
	case spansql.Timestamp:
		return "'0001-01-01 00:00:00'"
	default:
		return "''"
	}
}

func (u Update) SQL() string {
	return fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s IS NULL", u.Table, u.Def.Name, u.defaultValue(), u.Def.Name)
}
