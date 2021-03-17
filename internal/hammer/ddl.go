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
	ddl, err := spansql.ParseDDL(uri, schema)
	if err != nil {
		return DDL{}, fmt.Errorf("%s failed to parse ddl: %s", uri, err)
	}
	list := make([]Statement, len(ddl.List))
	for i, stmt := range ddl.List {
		list[i] = stmt
	}
	return DDL{List: list}, nil
}

type AlterColumn struct {
	Table      spansql.ID
	Def        spansql.ColumnDef
	SetOptions bool
}

func (a AlterColumn) SQL() string {
	str := "ALTER TABLE " + a.Table.SQL() + " ALTER COLUMN " + spansql.ID(a.Def.Name).SQL()

	if a.SetOptions {
		if a.Def.Options.AllowCommitTimestamp != nil && *a.Def.Options.AllowCommitTimestamp {
			str += " SET OPTIONS (allow_commit_timestamp = true)"
		} else {
			str += " SET OPTIONS (allow_commit_timestamp = null)"
		}
	} else {
		str += " " + a.Def.Type.SQL()
		if a.Def.NotNull {
			str += " NOT NULL"
		}
	}
	return str
}

type Update struct {
	Table spansql.ID
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
	case spansql.String:
		return "''"
	case spansql.Bytes:
		return "b''"
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
