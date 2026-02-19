package hammer

import (
	"fmt"
	"strings"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
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

	ddls, err := memefish.ParseDDLs(uri, strings.Join(lines, ""))
	if err != nil {
		return DDL{}, fmt.Errorf("%s failed to parse ddl: %s", uri, err)
	}
	list := make([]Statement, 0, len(ddls))
	for _, stmt := range ddls {
		if _, ok := stmt.(*ast.AlterDatabase); ok && option.IgnoreAlterDatabase {
			continue
		}
		if _, ok := stmt.(*ast.CreateChangeStream); ok && option.IgnoreChangeStreams {
			continue
		}
		if _, ok := stmt.(*ast.CreateModel); ok && option.IgnoreModels {
			continue
		}
		if _, ok := stmt.(*ast.CreateSequence); ok && option.IgnoreSequences {
			continue
		}
		list = append(list, stmt)
	}
	return DDL{List: list}, nil
}

type AlterColumn struct {
	Table      string
	Def        *ast.ColumnDef
	SetOptions bool
}

func (a AlterColumn) SQL() string {
	str := "ALTER TABLE " + a.Table + " ALTER COLUMN " + a.Def.Name.SQL()

	if a.SetOptions {
		v := optionsValueFromName(a.Def.Options, "allow_commit_timestamp")
		var allowCommitTimestamp bool
		if v != nil {
			vt, ok := (*v).(*ast.BoolLiteral)
			if ok {
				allowCommitTimestamp = vt.Value
			}
		}
		if allowCommitTimestamp {
			return str + " SET OPTIONS (allow_commit_timestamp = true)"
		} else {
			return str + " SET OPTIONS (allow_commit_timestamp = null)"
		}
	}

	str += " " + a.Def.Type.SQL()
	if a.Def.NotNull {
		str += " NOT NULL"
	}

	if a.Def.DefaultSemantics != nil {
		str += " " + a.Def.DefaultSemantics.SQL()
	}

	// HIDDEN will be either be 0 or invalid if not set.
	if !a.Def.Hidden.Invalid() && a.Def.Hidden != 0 {
		str += " HIDDEN"
	}

	return str
}

type Update struct {
	Table string
	Def   *ast.ColumnDef
}

func (u Update) defaultValue() string {
	if u.Def.DefaultSemantics != nil {
		switch t := u.Def.DefaultSemantics.(type) {
		case *ast.ColumnDefaultExpr:
			return t.Expr.SQL()
		}
	}

	switch t := u.Def.Type.(type) {
	case *ast.ArraySchemaType:
		return "[]"
	case *ast.ScalarSchemaType:
		return defaultByScalarTypeName(t.Name).SQL()
	case *ast.SizedSchemaType:
		return defaultByScalarTypeName(t.Name).SQL()
	default:
		return "''"
	}
}

func (u Update) SQL() string {
	return fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s IS NULL", u.Table, u.Def.Name.SQL(), u.defaultValue(), u.Def.Name.SQL())
}
