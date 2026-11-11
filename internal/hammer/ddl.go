package hammer

import (
	"fmt"
	"slices"
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
		if _, ok := stmt.(*ast.CreateProtoBundle); ok && option.IgnoreProtoBundles {
			continue
		}
		if seq, ok := stmt.(*ast.CreateSequence); ok {
			normalized, err := normalizeCreateSequence(seq)
			if err != nil {
				return DDL{}, fmt.Errorf("%s: %w", uri, err)
			}
			stmt = normalized
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
	case *ast.ScalarSchemaType, *ast.SizedSchemaType:
		if expr, err := zeroValue(t); err == nil {
			return expr.SQL()
		}
	}
	return "''"
}

func (u Update) SQL() string {
	return fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s IS NULL", u.Table, u.Def.Name.SQL(), u.defaultValue(), u.Def.Name.SQL())
}

const (
	sequenceOptionKind             = "sequence_kind"
	sequenceOptionSkipRangeMin     = "skip_range_min"
	sequenceOptionSkipRangeMax     = "skip_range_max"
	sequenceOptionStartWithCounter = "start_with_counter"
)

var sequenceOptionOrder = []string{
	sequenceOptionKind,
	sequenceOptionSkipRangeMin,
	sequenceOptionSkipRangeMax,
	sequenceOptionStartWithCounter,
}

// memefish does not render the clause form (BIT_REVERSED_POSITIVE, SKIP RANGE,
// START COUNTER WITH) back into valid SQL, so it is rewritten into OPTIONS.
func normalizeCreateSequence(stmt *ast.CreateSequence) (*ast.CreateSequence, error) {
	if len(stmt.Params) == 0 {
		return stmt, nil
	}

	options := make(map[string]ast.Expr)
	for _, param := range stmt.Params {
		switch p := param.(type) {
		case *ast.BitReversedPositive:
			options[sequenceOptionKind] = &ast.StringLiteral{Value: "bit_reversed_positive"}
		case *ast.SkipRange:
			options[sequenceOptionSkipRangeMin] = p.Min
			options[sequenceOptionSkipRangeMax] = p.Max
		case *ast.StartCounterWith:
			options[sequenceOptionStartWithCounter] = p.Counter
		default:
			return nil, fmt.Errorf("unsupported sequence parameter %s in %s", param.SQL(), stmt.Name.SQL())
		}
	}
	for name, value := range sequenceOptions(stmt.Options) {
		options[name] = value
	}

	var records []*ast.OptionsDef
	for _, name := range sequenceOptionNames(options) {
		records = append(records, &ast.OptionsDef{Name: &ast.Ident{Name: name}, Value: options[name]})
	}
	return &ast.CreateSequence{
		Create:      stmt.Create,
		Name:        stmt.Name,
		IfNotExists: stmt.IfNotExists,
		Options:     &ast.Options{Records: records},
	}, nil
}

func sequenceOptions(options *ast.Options) map[string]ast.Expr {
	m := make(map[string]ast.Expr)
	if options == nil {
		return m
	}
	for _, r := range options.Records {
		name := strings.ToLower(r.Name.Name)
		if _, ok := r.Value.(*ast.NullLiteral); ok {
			delete(m, name)
			continue
		}
		value := r.Value
		if lit, ok := value.(*ast.StringLiteral); ok && name == sequenceOptionKind {
			value = &ast.StringLiteral{Value: strings.ToLower(lit.Value)}
		}
		m[name] = value
	}
	return m
}

func sequenceOptionNames(options map[string]ast.Expr) []string {
	var names []string
	for _, name := range sequenceOptionOrder {
		if _, ok := options[name]; ok {
			names = append(names, name)
		}
	}
	var others []string
	for name := range options {
		if !slices.Contains(sequenceOptionOrder, name) {
			others = append(others, name)
		}
	}
	slices.Sort(others)
	return append(names, others...)
}
