package hammer

import (
	"fmt"
	"strings"

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

func ParseDDL(uri, schema string, option *DDLOption) (DDL, error) {
	var lines []string
	for _, line := range strings.Split(schema, ";") {
		trimed := strings.TrimSpace(line)
		if trimed == "" {
			continue
		}
		lines = append(lines, line+";")
	}

	ddl, err := spansql.ParseDDL(uri, strings.Join(lines, ""))
	if err != nil {
		return DDL{}, fmt.Errorf("%s failed to parse ddl: %s", uri, err)
	}
	list := make([]Statement, 0, len(ddl.List))
	for _, stmt := range ddl.List {
		if _, ok := stmt.(*spansql.AlterDatabase); ok && option.IgnoreAlterDatabase {
			continue
		}
		if _, ok := stmt.(*spansql.CreateChangeStream); ok && option.IgnoreChangeStreams {
			continue
		}
		list = append(list, stmt)
	}
	return DDL{List: list}, nil
}

type AlterColumn struct {
	Table      spansql.ID
	Def        spansql.ColumnDef
	SetOptions bool
}

func (a AlterColumn) SQL() string {
	str := "ALTER TABLE " + a.Table.SQL() + " ALTER COLUMN " + a.Def.Name.SQL()

	if a.SetOptions {
		if a.Def.Options.AllowCommitTimestamp != nil && *a.Def.Options.AllowCommitTimestamp {
			str += " SET OPTIONS (allow_commit_timestamp = true)"
		} else {
			str += " SET OPTIONS (allow_commit_timestamp = null)"
		}
		return str
	}

	str += " " + a.Def.Type.SQL()
	if a.Def.NotNull {
		str += " NOT NULL"
	}
	if a.Def.Default != nil {
		str += " DEFAULT (" + a.Def.Default.SQL() + ")"
	}

	return str
}

type Update struct {
	Table spansql.ID
	Def   spansql.ColumnDef
}

func (u Update) defaultValue() string {
	if u.Def.Default != nil {
		return u.Def.Default.SQL()
	}

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
		return "'0001-01-01T00:00:00Z'"
	case spansql.JSON:
		return "JSON '{}'"
	default:
		return "''"
	}
}

func (u Update) SQL() string {
	return fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s IS NULL", u.Table.SQL(), u.Def.Name.SQL(), u.defaultValue(), u.Def.Name.SQL())
}
