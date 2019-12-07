package main

import (
	"fmt"

	"cloud.google.com/go/spanner/spansql"
)

type AlterColumn struct {
	Table string
	Def   spansql.ColumnDef
}

func (a AlterColumn) SQL() string {
	return "ALTER TABLE " + a.Table + " ALTER COLUMN " + a.Def.SQL()
}

type UpdateColumn struct {
	Table string
	Def   spansql.ColumnDef
}

func (u UpdateColumn) defaultValue() string {
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

func (u UpdateColumn) SQL() string {
	return fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s IS NULL", u.Table, u.Def.Name, u.defaultValue(), u.Def.Name)
}
