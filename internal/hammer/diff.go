package hammer

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/cloudspannerecosystem/memefish/token"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func Diff(ddl1, ddl2 DDL) (DDL, error) {
	database1, err := NewDatabase(ddl1)
	if err != nil {
		return DDL{}, err
	}
	database2, err := NewDatabase(ddl2)
	if err != nil {
		return DDL{}, err
	}

	generator := &Generator{
		from:                             database1,
		to:                               database2,
		willCreateOrAlterChangeStreamIDs: map[string]*ChangeStream{},
	}
	return generator.GenerateDDL(), nil
}

func NewDatabase(ddl DDL) (*Database, error) {
	var (
		tables               []*Table
		changeStreams        []*ChangeStream
		views                []*View
		alterDatabaseOptions *ast.AlterDatabase
		options              *ast.Options
	)

	m := make(map[string]*Table)
	for _, istmt := range ddl.List {
		switch stmt := istmt.(type) {
		case *ast.CreateTable:
			t := &Table{CreateTable: stmt}
			tables = append(tables, t)
			m[identsToComparable(stmt.Name.Idents...)] = t
		case *ast.CreateIndex:
			if t, ok := m[identsToComparable(stmt.TableName.Idents...)]; ok {
				t.indexes = append(t.indexes, stmt)
			} else {
				return nil, fmt.Errorf("cannot find ddl of table to apply index %s", stmt.Name.SQL())
			}
		case *ast.CreateSearchIndex:
			if t, ok := m[identsToComparable(stmt.TableName)]; ok {
				t.searchIndexes = append(t.searchIndexes, stmt)
			} else {
				return nil, fmt.Errorf("cannot find ddl of table to apply search index %s", stmt.Name.SQL())
			}
		case *ast.AlterTable:
			t, ok := m[identsToComparable(stmt.Name.Idents...)]
			if !ok {
				return nil, fmt.Errorf("cannot find ddl of table to apply index %s", stmt.Name.SQL())
			}
			switch alteration := stmt.TableAlteration.(type) {
			case *ast.AddTableConstraint:
				t.TableConstraints = append(t.TableConstraints, alteration.TableConstraint)
			default:
				return nil, fmt.Errorf("unsupported table alteration: %v", stmt)
			}
		case *ast.AlterDatabase:
			alterDatabaseOptions = stmt
			options = stmt.Options
		case *ast.CreateChangeStream:
			switch forType := stmt.For.(type) {
			case *ast.ChangeStreamForTables:
				for _, table := range forType.Tables {
					if t, ok := m[identsToComparable(table.TableName)]; ok {
						t.changeStreams = append(t.changeStreams, &ChangeStream{CreateChangeStream: stmt})
					}
				}
			default:
				changeStreams = append(changeStreams, &ChangeStream{CreateChangeStream: stmt})
			}
		case *ast.CreateView:
			v := &View{CreateView: stmt}
			views = append(views, v)
		default:
			return nil, fmt.Errorf("unexpected ddl statement: %v", stmt.SQL())
		}
	}
	for _, t := range tables {
		if i := t.Cluster; i != nil {
			if p, ok := m[identsToComparable(i.TableName.Idents...)]; ok {
				p.children = append(p.children, t)
			} else {
				return nil, fmt.Errorf("parent ddl %s not found", i.TableName.SQL())
			}
		}
	}

	return &Database{tables: tables, changeStreams: changeStreams, views: views, alterDatabaseOptions: alterDatabaseOptions, options: options}, nil
}

type Database struct {
	tables        []*Table
	changeStreams []*ChangeStream
	views         []*View

	alterDatabaseOptions *ast.AlterDatabase
	options              *ast.Options
}

type Table struct {
	*ast.CreateTable

	indexes       []*ast.CreateIndex
	searchIndexes []*ast.CreateSearchIndex
	children      []*Table
	changeStreams []*ChangeStream
}

type View struct {
	*ast.CreateView
}

type ChangeStream struct {
	*ast.CreateChangeStream
}

type ChangeStreamType string

const (
	ChangeStreamTypeAll    ChangeStreamType = "ALL"
	ChangeStreamTypeNone   ChangeStreamType = "NONE"
	ChangeStreamTypeTables ChangeStreamType = "TABLES"
)

func (cs *ChangeStream) Type() ChangeStreamType {
	if cs.For == nil {
		return ChangeStreamTypeNone
	}
	switch cs.For.(type) {
	case *ast.ChangeStreamForTables:
		return ChangeStreamTypeTables
	default:
		return ChangeStreamTypeAll
	}
}

type Generator struct {
	from *Database
	to   *Database

	dropedTable                      []string
	dropedIndex                      []string
	dropedChangeStream               []string
	droppedConstraints               []*ast.TableConstraint
	willCreateOrAlterChangeStreamIDs map[string]*ChangeStream
}

func (g *Generator) GenerateDDL() DDL {
	ddl := DDL{}

	// for alter database
	ddl.AppendDDL(g.generateDDLForAlterDatabaseOptions())

	// for alter table
	for _, toTable := range g.to.tables {
		fromTable, exists := g.findTableByName(g.from.tables, identsToComparable(toTable.Name.Idents...))

		if !exists {
			ddl.AppendDDL(g.generateDDLForCreateTableAndIndex(toTable))
			continue
		}

		if g.isDropedTable(identsToComparable(toTable.Name.Idents...)) {
			ddl.AppendDDL(g.generateDDLForCreateTableAndIndex(toTable))
			continue
		}

		if !g.interleaveEqual(fromTable, toTable) {
			ddl.AppendDDL(g.generateDDLForDropConstraintIndexAndTable(fromTable))
			ddl.AppendDDL(g.generateDDLForCreateTableAndIndex(toTable))
			continue
		}

		if !g.primaryKeyEqual(fromTable, toTable) {
			ddl.AppendDDL(g.generateDDLForDropConstraintIndexAndTable(fromTable))
			ddl.AppendDDL(g.generateDDLForCreateTableAndIndex(toTable))
			continue
		}

		ddl.AppendDDL(g.generateDDLForDropIndex(fromTable, toTable))
		ddl.AppendDDL(g.generateDDLForColumns(fromTable, toTable))
		ddl.AppendDDL(g.generateDDLForCreateIndex(fromTable, toTable))
		ddl.AppendDDL(g.generateDDLForConstraints(fromTable, toTable))
		ddl.AppendDDL(g.generateDDLForRowDeletionPolicy(fromTable, toTable))
		ddl.AppendDDL(g.generateDDLForCreateChangeStream(g.from, toTable))
		ddl.AppendDDL(g.generateDDLForAlterIndex(fromTable, toTable))
	}
	for _, fromTable := range g.from.tables {
		if _, exists := g.findTableByName(g.to.tables, identsToComparable(fromTable.Name.Idents...)); !exists {
			ddl.AppendDDL(g.generateDDLForDropConstraintIndexAndTable(fromTable))
		}
	}
	// for alter change stream
	for _, toChangeStream := range g.to.changeStreams {
		fromChangeStream, exists := g.findChangeStreamByName(g.from, identsToComparable(toChangeStream.Name))
		if !exists {
			ddl.Append(toChangeStream)
			continue
		}
		ddl.AppendDDL(g.generateDDLForAlterChangeStream(fromChangeStream, toChangeStream))
	}
	for _, fromChangeStream := range g.from.changeStreams {
		if _, exists := g.findChangeStreamByName(g.to, identsToComparable(fromChangeStream.Name)); !exists {
			ddl.AppendDDL(g.generateDDLForDropChangeStream(fromChangeStream))
		}
	}
	for _, cs := range g.willCreateOrAlterChangeStreamIDs {
		fromChangeStream, exists := g.findChangeStreamByName(g.from, identsToComparable(cs.Name))
		if !exists || g.isDropedChangeStream(identsToComparable(cs.Name)) {
			ddl.Append(cs)
			continue
		}

		ddl.AppendDDL(g.generateDDLForAlterChangeStream(fromChangeStream, cs))
	}
	// for views
	for _, toView := range g.to.views {
		_, exists := g.findViewByName(g.from.views, identsToComparable(toView.Name.Idents...))
		if !exists {
			ddl.Append(toView)
			continue
		}
		ddl.AppendDDL(g.generateDDLForReplaceView(toView))
	}
	for _, fromView := range g.from.views {
		if _, exists := g.findViewByName(g.to.views, identsToComparable(fromView.Name.Idents...)); !exists {
			ddl.AppendDDL(g.generateDDLForDropView(fromView))
		}
	}
	return ddl
}

func (g *Generator) generateDDLForAlterDatabaseOptions() DDL {
	ddl := DDL{}
	optionsFrom := make(map[string]string)
	optionsTo := make(map[string]string)
	if g.from.options != nil {
		for _, o := range g.from.options.Records {
			optionsFrom[o.Name.Name] = o.Value.SQL()
		}
	}
	if g.to.options != nil {
		for _, o := range g.to.options.Records {
			optionsTo[o.Name.Name] = o.Value.SQL()
		}
	}
	if reflect.DeepEqual(optionsFrom, optionsTo) {
		return ddl
	}
	if g.to.alterDatabaseOptions == nil {
		// set all null
		ddl.Append(&ast.AlterDatabase{
			Name: g.from.alterDatabaseOptions.Name,
			Options: &ast.Options{
				Records: []*ast.OptionsDef{
					{
						Name:  &ast.Ident{Name: "optimizer_version"},
						Value: &ast.NullLiteral{},
					},
					{
						Name:  &ast.Ident{Name: "version_retention_period"},
						Value: &ast.NullLiteral{},
					},
					{
						Name:  &ast.Ident{Name: "enable_key_visualizer"},
						Value: &ast.NullLiteral{},
					},
				},
			},
		})
		return ddl
	}

	dbopts := g.to.alterDatabaseOptions.Options
	if g.from.options != nil {
		for _, r := range g.from.options.Records {
			name := r.Name.Name
			if _, ok := optionsTo[name]; ok {
				continue
			}
			dbopts.Records = append(dbopts.Records, &ast.OptionsDef{
				Name:  &ast.Ident{Name: name},
				Value: &ast.NullLiteral{},
			})
		}
	}

	ddl.Append(&ast.AlterDatabase{
		Name:    g.to.alterDatabaseOptions.Name,
		Options: dbopts,
	})
	return ddl
}

func (g *Generator) generateDDLForCreateTableAndIndex(table *Table) DDL {
	ddl := DDL{}

	ddl.Append(table)
	for _, i := range table.indexes {
		ddl.Append(i)
	}
	for _, i := range table.searchIndexes {
		ddl.Append(i)
	}
	for _, cs := range table.changeStreams {
		g.willCreateOrAlterChangeStreamIDs[identsToComparable(cs.Name)] = cs
	}
	return ddl
}

func (g *Generator) generateDDLForDropConstraintIndexAndTable(table *Table) DDL {
	ddl := DDL{}

	if g.isDropedTable(identsToComparable(table.Name.Idents...)) {
		return ddl
	}
	for _, t := range table.children {
		ddl.AppendDDL(g.generateDDLForDropConstraintIndexAndTable(t))
	}
	for _, i := range table.indexes {
		ddl.Append(&ast.DropIndex{Name: i.Name})
	}
	for _, i := range table.searchIndexes {
		ddl.Append(&ast.DropSearchIndex{Name: i.Name})
	}
	for _, cs := range table.changeStreams {
		if !g.isDropedChangeStream(identsToComparable(cs.Name)) {
			ddl.Append(&ast.DropChangeStream{Name: cs.Name})
			g.dropedChangeStream = append(g.dropedChangeStream, identsToComparable(cs.Name))
		}
	}
	ddl.AppendDDL(g.generateDDLForDropNamedConstraintsMatchingPredicate(func(constraint *ast.TableConstraint) bool {
		fk, ok := constraint.Constraint.(*ast.ForeignKey)
		if !ok {
			return false
		}
		return identsToComparable(fk.ReferenceTable.Idents...) == identsToComparable(table.Name.Idents...)
	}))
	ddl.Append(&ast.DropTable{Name: table.Name})
	g.dropedTable = append(g.dropedTable, identsToComparable(table.Name.Idents...))
	return ddl
}

func (g *Generator) generateDDLForConstraints(from, to *Table) DDL {
	ddl := DDL{}

	for _, toConstraint := range to.TableConstraints {
		isUnnamedConstraint := toConstraint.Name == nil

		if isUnnamedConstraint {
			_, exists := g.findUnnamedConstraint(from.TableConstraints, toConstraint)
			if !exists {
				ddl.Append(&ast.AlterTable{Name: to.Name, TableAlteration: &ast.AddTableConstraint{TableConstraint: toConstraint}})
			}
			continue
		}

		fromConstraint, exists := g.findNamedConstraint(from.TableConstraints, identsToComparable(toConstraint.Name))

		if !exists || g.isDroppedConstraint(toConstraint) {
			ddl.Append(&ast.AlterTable{Name: to.Name, TableAlteration: &ast.AddTableConstraint{TableConstraint: toConstraint}})
			continue
		}

		if g.constraintEqual(fromConstraint, toConstraint) {
			continue
		}

		ddl.AppendDDL(g.generateDDLForDropNamedConstraint(from.Name, fromConstraint))
		ddl.Append(&ast.AlterTable{
			Name:            to.Name,
			TableAlteration: &ast.AddTableConstraint{TableConstraint: toConstraint},
		})
	}

	for _, fromConstraint := range from.TableConstraints {
		if fromConstraint.Name == nil {
			continue
		}

		if _, exists := g.findNamedConstraint(to.TableConstraints, identsToComparable(fromConstraint.Name)); !exists {
			ddl.AppendDDL(g.generateDDLForDropNamedConstraint(from.Name, fromConstraint))
		}
	}
	return ddl
}

func (g *Generator) generateDDLForRowDeletionPolicy(from, to *Table) DDL {
	ddl := DDL{}

	switch {
	case from.RowDeletionPolicy != nil && to.RowDeletionPolicy != nil:
		if g.createRowDeletionPolicyEqual(from.RowDeletionPolicy, to.RowDeletionPolicy) {
			return ddl
		}
		ddl.Append(&ast.AlterTable{
			Name: to.Name,
			TableAlteration: &ast.ReplaceRowDeletionPolicy{
				RowDeletionPolicy: to.RowDeletionPolicy.RowDeletionPolicy,
			},
		})
	case from.RowDeletionPolicy != nil && to.RowDeletionPolicy == nil:
		ddl.Append(&ast.AlterTable{
			Name:            to.Name,
			TableAlteration: &ast.DropRowDeletionPolicy{},
		})
	case from.RowDeletionPolicy == nil && to.RowDeletionPolicy != nil:
		ddl.Append(&ast.AlterTable{
			Name: to.Name,
			TableAlteration: &ast.AddRowDeletionPolicy{
				RowDeletionPolicy: to.RowDeletionPolicy.RowDeletionPolicy,
			},
		})
	}

	return ddl
}

func (g *Generator) generateDDLForColumns(from, to *Table) DDL {
	ddl := DDL{}

	for _, toCol := range to.Columns {
		fromCol, exists := g.findColumnByName(from.Columns, identsToComparable(toCol.Name))

		if !exists {
			if toCol.NotNull && toCol.DefaultSemantics == nil {
				ddl.Append(&ast.AlterTable{Name: to.Name, TableAlteration: &ast.AddColumn{Column: g.setDefaultSemantics(toCol)}})
				ddl.Append(&ast.AlterTable{Name: to.Name, TableAlteration: &ast.AlterColumn{Name: toCol.Name, Alteration: &ast.AlterColumnDropDefault{}}})
			} else {
				ddl.Append(&ast.AlterTable{Name: to.Name, TableAlteration: &ast.AddColumn{Column: toCol}})
			}
			continue
		}

		if fromCol.Hidden != toCol.Hidden {
			ddl.Append(AlterColumn{Table: to.Name.SQL(), Def: toCol})
			continue
		}

		if g.columnDefEqual(fromCol, toCol) {
			continue
		}

		requireDropAndCreateByDefault := func(d ast.ColumnDefaultSemantics) bool {
			if d == nil {
				return false
			}
			switch d.(type) {
			case *ast.ColumnDefaultExpr:
				return false
			default:
				return true
			}
		}
		if g.columnTypeEqual(fromCol, toCol) && !requireDropAndCreateByDefault(fromCol.DefaultSemantics) && !requireDropAndCreateByDefault(toCol.DefaultSemantics) {
			if st, ok := fromCol.Type.(*ast.ScalarSchemaType); ok && st.Name == ast.TimestampTypeName {
				if fromCol.NotNull != toCol.NotNull || !g.columnDefaultExprEqual(fromCol.DefaultSemantics, toCol.DefaultSemantics) {
					if !fromCol.NotNull && toCol.NotNull {
						ddl.Append(Update{Table: to.Name.SQL(), Def: toCol})
					}
					ddl.Append(AlterColumn{Table: to.Name.SQL(), Def: toCol})
				}
				if !g.optionsValueEqual(fromCol.Options, toCol.Options, "allow_commit_timestamp") {
					ddl.Append(AlterColumn{Table: to.Name.SQL(), Def: toCol, SetOptions: true})
				}
			} else {
				if !fromCol.NotNull && toCol.NotNull {
					ddl.Append(Update{Table: to.Name.SQL(), Def: toCol})
				}
				ddl.Append(AlterColumn{Table: to.Name.SQL(), Def: toCol})
			}
		} else {
			ddl.AppendDDL(g.generateDDLForDropAndCreateColumn(from, to, fromCol, toCol))
		}
	}
	for _, fromCol := range from.Columns {
		if _, exists := g.findColumnByName(to.Columns, identsToComparable(fromCol.Name)); !exists {
			ddl.AppendDDL(g.generateDDLForDropColumn(from.Name, fromCol.Name))
		}
	}
	return ddl
}

func (g *Generator) generateDDLForDropColumn(table *ast.Path, column *ast.Ident) DDL {
	ddl := DDL{}

	ddl.AppendDDL(g.generateDDLForDropNamedConstraintsMatchingPredicate(func(constraint *ast.TableConstraint) bool {
		fk, ok := constraint.Constraint.(*ast.ForeignKey)
		if !ok {
			return false
		}
		for _, c := range fk.Columns {
			if identsToComparable(column) == identsToComparable(c) {
				return true
			}
		}

		for _, refColumn := range fk.ReferenceColumns {
			if identsToComparable(column) == identsToComparable(refColumn) {
				return true
			}
		}

		return false
	}))

	ddl.Append(&ast.AlterTable{
		Name:            table,
		TableAlteration: &ast.DropColumn{Name: column},
	})

	return ddl
}

func (g *Generator) generateDDLForDropAndCreateColumn(from, to *Table, fromCol, toCol *ast.ColumnDef) DDL {
	ddl := DDL{}

	indexes := []*ast.CreateIndex{}
	for _, i := range g.findIndexByColumn(from.indexes, identsToComparable(fromCol.Name)) {
		if !g.isDropedIndex(identsToComparable(i.Name.Idents...)) {
			indexes = append(indexes, i)
		}
	}
	for _, i := range indexes {
		ddl.Append(&ast.DropIndex{Name: i.Name})
	}

	searchIndexes := []*ast.CreateSearchIndex{}
	for _, i := range g.findSearchIndexByColumn(from.searchIndexes, identsToComparable(fromCol.Name)) {
		if !g.isDropedIndex(identsToComparable(i.Name)) {
			searchIndexes = append(searchIndexes, i)
		}
	}
	for _, i := range searchIndexes {
		ddl.Append(&ast.DropSearchIndex{Name: i.Name})
	}

	ddl.AppendDDL(g.generateDDLForDropColumn(from.Name, fromCol.Name))

	if toCol.NotNull && toCol.DefaultSemantics == nil {
		ddl.Append(&ast.AlterTable{Name: to.Name, TableAlteration: &ast.AddColumn{Column: g.setDefaultSemantics(toCol)}})
		ddl.Append(&ast.AlterTable{Name: to.Name, TableAlteration: &ast.AlterColumn{Name: toCol.Name, Alteration: &ast.AlterColumnDropDefault{}}})
	} else {
		ddl.Append(&ast.AlterTable{Name: to.Name, TableAlteration: &ast.AddColumn{Column: toCol}})
	}
	for _, i := range indexes {
		ddl.Append(i)
	}
	for _, i := range searchIndexes {
		ddl.Append(i)
	}
	return ddl
}

func (g *Generator) generateDDLForDropIndex(from, to *Table) DDL {
	ddl := DDL{}

	for _, toIndex := range to.indexes {
		fromIndex, exists := g.findIndexByName(from.indexes, identsToComparable(toIndex.Name.Idents...))

		if exists && !g.indexEqual(fromIndex, toIndex) {
			ddl.Append(&ast.DropIndex{Name: fromIndex.Name})
			g.dropedIndex = append(g.dropedIndex, identsToComparable(fromIndex.Name.Idents...))
		}
	}
	for _, fromIndex := range from.indexes {
		if _, exists := g.findIndexByName(to.indexes, identsToComparable(fromIndex.Name.Idents...)); !exists {
			ddl.Append(&ast.DropIndex{Name: fromIndex.Name})
			g.dropedIndex = append(g.dropedIndex, identsToComparable(fromIndex.Name.Idents...))
		}
	}

	for _, toIndex := range to.searchIndexes {
		fromIndex, exists := g.findSearchIndexByName(from.searchIndexes, identsToComparable(toIndex.Name))

		if exists && !g.searchIndexEqual(*fromIndex, *toIndex) {
			ddl.Append(&ast.DropSearchIndex{Name: fromIndex.Name})
			g.dropedIndex = append(g.dropedIndex, identsToComparable(fromIndex.Name))
		}
	}
	for _, fromIndex := range from.searchIndexes {
		if _, exists := g.findSearchIndexByName(to.searchIndexes, identsToComparable(fromIndex.Name)); !exists {
			ddl.Append(&ast.DropSearchIndex{Name: fromIndex.Name})
			g.dropedIndex = append(g.dropedIndex, identsToComparable(fromIndex.Name))
		}
	}

	return ddl
}

func (g *Generator) generateDDLForCreateIndex(from, to *Table) DDL {
	ddl := DDL{}

	for _, toIndex := range to.indexes {
		fromIndex, exists := g.findIndexByName(from.indexes, identsToComparable(toIndex.Name.Idents...))

		if !exists || !g.indexEqual(fromIndex, toIndex) {
			ddl.Append(toIndex)
		}
	}

	for _, toIndex := range to.searchIndexes {
		fromIndex, exists := g.findSearchIndexByName(from.searchIndexes, identsToComparable(toIndex.Name))

		if !exists || !g.searchIndexEqual(*fromIndex, *toIndex) {
			ddl.Append(toIndex)
		}
	}

	return ddl
}

func (g *Generator) generateDDLForAlterIndex(from, to *Table) DDL {
	ddl := DDL{}

	for _, toIndex := range to.indexes {
		if toIndex.Storing == nil {
			continue
		}

		fromIndex, exists := g.findIndexByName(from.indexes, identsToComparable(toIndex.Name.Idents...))
		if !exists || !g.indexEqual(fromIndex, toIndex) {
			continue
		}

		for _, toIndexStoringColumn := range toIndex.Storing.Columns {
			if fromIndex.Storing != nil {
				if _, exists := g.findIdentByName(fromIndex.Storing.Columns, identsToComparable(toIndexStoringColumn)); exists {
					continue
				}
			}
			ddl.Append(&ast.AlterIndex{
				Name: toIndex.Name,
				IndexAlteration: &ast.AddStoredColumn{
					Name: toIndexStoringColumn,
				},
			})
		}
	}

	for _, fromIndex := range from.indexes {
		if fromIndex.Storing == nil {
			continue
		}

		toIndex, exists := g.findIndexByName(to.indexes, identsToComparable(fromIndex.Name.Idents...))
		if !exists || !g.indexEqual(fromIndex, toIndex) {
			continue
		}

		for _, fromIndexStoringColumn := range fromIndex.Storing.Columns {
			if toIndex.Storing != nil {
				if _, exists := g.findIdentByName(toIndex.Storing.Columns, identsToComparable(fromIndexStoringColumn)); exists {
					continue
				}
			}
			ddl.Append(&ast.AlterIndex{
				Name: toIndex.Name,
				IndexAlteration: &ast.DropStoredColumn{
					Name: fromIndexStoringColumn,
				},
			})
		}
	}

	return ddl
}

func (g *Generator) generateDDLForCreateChangeStream(from *Database, to *Table) DDL {
	ddl := DDL{}

	for _, cs := range to.changeStreams {
		g.willCreateOrAlterChangeStreamIDs[identsToComparable(cs.Name)] = cs
	}
	return ddl
}

func (g *Generator) generateDDLForDropNamedConstraint(table *ast.Path, constraint *ast.TableConstraint) DDL {
	ddl := DDL{}

	if constraint.Name == nil {
		return ddl
	}

	for _, c := range g.droppedConstraints {
		if g.constraintEqual(c, constraint) {
			return ddl
		}
	}
	g.droppedConstraints = append(g.droppedConstraints, constraint)

	ddl.Append(&ast.AlterTable{
		Name: table,
		TableAlteration: &ast.DropConstraint{
			Name: constraint.Name,
		},
	})

	return ddl
}

func (g *Generator) isDropedTable(name string) bool {
	for _, t := range g.dropedTable {
		if t == name {
			return true
		}
	}
	return false
}

func (g *Generator) isDropedIndex(name string) bool {
	for _, t := range g.dropedIndex {
		if t == name {
			return true
		}
	}
	return false
}

func (g *Generator) isDroppedConstraint(constraint *ast.TableConstraint) bool {
	for _, c := range g.droppedConstraints {
		if g.constraintEqual(c, constraint) {
			return true
		}
	}
	return false
}

func (g *Generator) isDropedChangeStream(name string) bool {
	for _, t := range g.dropedChangeStream {
		if t == name {
			return true
		}
	}
	return false
}

func (g *Generator) interleaveEqual(x, y *Table) bool {
	return cmp.Equal(x.Cluster, y.Cluster, cmpopts.IgnoreTypes(token.Pos(0)))
}

func (g *Generator) primaryKeyEqual(x, y *Table) bool {
	if !cmp.Equal(x.PrimaryKeys, y.PrimaryKeys,
		cmpopts.IgnoreTypes(token.Pos(0)),
		cmp.Comparer(func(a, b *ast.IndexKey) bool {
			aVal := *a
			bVal := *b
			if aVal.Dir == "" {
				aVal.Dir = ast.DirectionAsc
			}
			if bVal.Dir == "" {
				bVal.Dir = ast.DirectionAsc
			}
			return cmp.Equal(aVal, bVal, cmpopts.IgnoreTypes(token.Pos(0)))
		})) {
		return false
	}
	for _, pk := range y.PrimaryKeys {
		xCol, exists := g.findColumnByName(x.Columns, identsToComparable(pk.Name))
		if !exists {
			return false
		}
		yCol, exists := g.findColumnByName(y.Columns, identsToComparable(pk.Name))
		if !exists {
			return false
		}
		if !g.columnDefEqual(xCol, yCol) {
			return false
		}
	}
	return true
}

func (g *Generator) columnDefEqual(x, y *ast.ColumnDef) bool {
	return cmp.Equal(x, y,
		cmpopts.IgnoreTypes(token.Pos(0)),
		cmp.Comparer(func(x, y *ast.Ident) bool {
			return strings.EqualFold(x.Name, y.Name)
		}),
		cmp.Comparer(func(x, y *ast.TimestampLiteral) bool {
			return x.Value.SQL() == y.Value.SQL()
		}),
	)
}

func (g *Generator) columnTypeEqual(x, y *ast.ColumnDef) bool {
	return cmp.Equal(x.Type, y.Type,
		cmpopts.IgnoreTypes(token.Pos(0)),
		cmp.Comparer(func(x, y *ast.SizedSchemaType) bool {
			return x.Name == y.Name
		}),
	)
}

func (g *Generator) constraintEqual(x, y *ast.TableConstraint) bool {
	return cmp.Equal(x, y, cmpopts.IgnoreTypes(token.Pos(0)))
}

func (g *Generator) indexEqual(x, y *ast.CreateIndex) bool {
	return cmp.Equal(x, y,
		cmpopts.IgnoreTypes(token.Pos(0)),
		cmpopts.IgnoreTypes(&ast.Storing{}),
		cmp.Comparer(func(a, b *ast.IndexKey) bool {
			aVal := *a
			bVal := *b
			if aVal.Dir == "" {
				aVal.Dir = ast.DirectionAsc
			}
			if bVal.Dir == "" {
				bVal.Dir = ast.DirectionAsc
			}
			return cmp.Equal(aVal, bVal, cmpopts.IgnoreTypes(token.Pos(0)))
		}),
	)
}

func (g *Generator) searchIndexEqual(x, y ast.CreateSearchIndex) bool {
	return cmp.Equal(x, y, cmpopts.IgnoreTypes(token.Pos(0)))
}

func (g *Generator) changeStreamForEqual(x, y ast.ChangeStreamFor) bool {
	return cmp.Equal(x, y, cmpopts.IgnoreTypes(token.Pos(0)))
}

func (g *Generator) columnDefaultExprEqual(x, y ast.ColumnDefaultSemantics) bool {
	return cmp.Equal(x, y, cmpopts.IgnoreTypes(token.Pos(0)))
}

func (g *Generator) createRowDeletionPolicyEqual(x, y *ast.CreateRowDeletionPolicy) bool {
	return cmp.Equal(x, y, cmpopts.IgnoreTypes(token.Pos(0)))
}

func (g *Generator) optionsEqual(x, y *ast.Options) bool {
	return cmp.Equal(x, y, cmpopts.IgnoreTypes(token.Pos(0)))
}

func (g *Generator) optionsValueEqual(x, y *ast.Options, name string) bool {
	xv := optionsValueFromName(x, name)
	yv := optionsValueFromName(y, name)
	return cmp.Equal(xv, yv, cmpopts.IgnoreTypes(token.Pos(0)))
}

func optionsValueFromName(options *ast.Options, name string) *ast.Expr {
	if options == nil {
		return nil
	}
	for _, o := range options.Records {
		if o.Name.Name == name {
			return &o.Value
		}
	}
	return nil
}

func defaultByScalarTypeName(t ast.ScalarTypeName) ast.Expr {
	switch t {
	case ast.BoolTypeName:
		return &ast.BoolLiteral{Value: false}
	case ast.Int64TypeName:
		return &ast.IntLiteral{Value: "0"}
	case ast.Float32TypeName, ast.Float64TypeName:
		return &ast.FloatLiteral{Value: "0"}
	case ast.StringTypeName:
		return &ast.StringLiteral{Value: ""}
	case ast.BytesTypeName:
		return &ast.BytesLiteral{Value: nil}
	case ast.DateTypeName:
		return &ast.DateLiteral{Value: &ast.StringLiteral{Value: "0001-01-01"}}
	case ast.TimestampTypeName:
		return &ast.TimestampLiteral{Value: &ast.StringLiteral{Value: "0001-01-01T00:00:00Z"}}
	case ast.NumericTypeName:
		return &ast.NumericLiteral{Value: &ast.StringLiteral{Value: "0"}}
	case ast.JSONTypeName:
		return &ast.JSONLiteral{Value: &ast.StringLiteral{Value: "{}"}}
	case ast.TokenListTypeName:
		return &ast.BytesLiteral{Value: nil}
	default:
		panic("not implemented")
	}
}

func identsToComparable(is ...*ast.Ident) string {
	var b strings.Builder
	for i, n := range is {
		if i != 0 {
			b.WriteByte('.')
		}
		b.WriteString(n.Name)
	}
	return b.String()
}

func (g *Generator) setDefaultSemantics(col *ast.ColumnDef) *ast.ColumnDef {
	switch t := col.Type.(type) {
	case *ast.ArraySchemaType:
		col.DefaultSemantics = &ast.ColumnDefaultExpr{Expr: &ast.ArrayLiteral{Values: nil}}
	case *ast.ScalarSchemaType:
		col.DefaultSemantics = &ast.ColumnDefaultExpr{Expr: defaultByScalarTypeName(t.Name)}
	case *ast.SizedSchemaType:
		col.DefaultSemantics = &ast.ColumnDefaultExpr{Expr: defaultByScalarTypeName(t.Name)}
	case *ast.NamedType:
		panic("not implemented")
	}

	return col
}

func (g *Generator) findTableByName(tables []*Table, name string) (table *Table, exists bool) {
	for _, t := range tables {
		if strings.EqualFold(identsToComparable(t.Name.Idents...), name) {
			table = t
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findColumnByName(cols []*ast.ColumnDef, name string) (col *ast.ColumnDef, exists bool) {
	for _, c := range cols {
		if strings.EqualFold(identsToComparable(c.Name), name) {
			col = c
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findIdentByName(idents []*ast.Ident, name string) (ident *ast.Ident, exists bool) {
	for _, i := range idents {
		if strings.EqualFold(identsToComparable(i), name) {
			ident = i
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findNamedConstraint(constraints []*ast.TableConstraint, name string) (con *ast.TableConstraint, exists bool) {
	if name == "" {
		exists = false
		return
	}

	for _, c := range constraints {
		if identsToComparable(c.Name) == name {
			con = c
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findUnnamedConstraint(constraints []*ast.TableConstraint, item *ast.TableConstraint) (con *ast.TableConstraint, exists bool) {
	for _, c := range constraints {
		if g.constraintEqual(c, item) {
			con = c
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findIndexByName(indexes []*ast.CreateIndex, name string) (index *ast.CreateIndex, exists bool) {
	for _, i := range indexes {
		if identsToComparable(i.Name.Idents...) == name {
			return i, true
		}
	}
	return nil, false
}

func (g *Generator) findIndexByColumn(indexes []*ast.CreateIndex, column string) []*ast.CreateIndex {
	result := []*ast.CreateIndex{}
	for _, i := range indexes {
		for _, c := range i.Keys {
			if identsToComparable(c.Name) == column {
				result = append(result, i)
				break
			}
		}
	}
	return result
}

func (g *Generator) findSearchIndexByName(indexes []*ast.CreateSearchIndex, name string) (index *ast.CreateSearchIndex, exists bool) {
	for _, i := range indexes {
		if identsToComparable(i.Name) == name {
			return i, true
		}
	}
	return nil, false
}

func (g *Generator) findSearchIndexByColumn(indexes []*ast.CreateSearchIndex, column string) []*ast.CreateSearchIndex {
	result := []*ast.CreateSearchIndex{}
	for _, i := range indexes {
		for _, c := range i.TokenListPart {
			if c.Name == column {
				result = append(result, i)
				break
			}
		}
	}
	return result
}

func (g *Generator) generateDDLForDropNamedConstraintsMatchingPredicate(predicate func(constraint *ast.TableConstraint) bool) DDL {
	ddl := DDL{}

	for _, table := range g.from.tables {
		for _, constraint := range table.TableConstraints {
			if predicate(constraint) {
				ddl.AppendDDL(g.generateDDLForDropNamedConstraint(table.Name, constraint))
			}
		}
	}

	return ddl
}

func (g *Generator) findChangeStreamByName(database *Database, name string) (changeStream *ChangeStream, exists bool) {
	for _, cs := range database.changeStreams {
		if identsToComparable(cs.Name) == name {
			changeStream = cs
			exists = true
			break
		}
	}
	for _, table := range database.tables {
		for _, cs := range table.changeStreams {
			if identsToComparable(cs.Name) == name {
				changeStream = cs
				exists = true
				break
			}
		}
	}
	return
}

func (g *Generator) generateDDLForAlterChangeStream(from, to *ChangeStream) DDL {
	ddl := DDL{}
	fromType, toType := from.Type(), to.Type()
	switch {
	case fromType == ChangeStreamTypeAll && toType == ChangeStreamTypeTables:
		ddl.Append(&ast.AlterChangeStream{Name: to.Name, ChangeStreamAlteration: &ast.ChangeStreamSetFor{For: to.For}})
	case fromType == ChangeStreamTypeAll && toType == ChangeStreamTypeNone:
		ddl.Append(&ast.AlterChangeStream{Name: to.Name, ChangeStreamAlteration: &ast.ChangeStreamDropForAll{}})
	case fromType == ChangeStreamTypeTables && toType == ChangeStreamTypeAll:
		ddl.Append(&ast.AlterChangeStream{Name: to.Name, ChangeStreamAlteration: &ast.ChangeStreamSetFor{For: to.For}})
	case fromType == ChangeStreamTypeTables && toType == ChangeStreamTypeNone:
		ddl.Append(&ast.DropChangeStream{Name: to.Name})
		ddl.Append(&ast.CreateChangeStream{Name: to.Name})
	case fromType == ChangeStreamTypeTables && toType == ChangeStreamTypeTables:
		if !g.changeStreamForEqual(from.For, to.For) {
			ddl.Append(&ast.AlterChangeStream{Name: to.Name, ChangeStreamAlteration: &ast.ChangeStreamSetFor{For: to.For}})
		}
	case fromType == ChangeStreamTypeNone && toType == ChangeStreamTypeAll:
		ddl.Append(&ast.AlterChangeStream{Name: to.Name, ChangeStreamAlteration: &ast.ChangeStreamSetFor{For: to.For}})
	case fromType == ChangeStreamTypeNone && toType == ChangeStreamTypeTables:
		ddl.Append(&ast.AlterChangeStream{Name: to.Name, ChangeStreamAlteration: &ast.ChangeStreamSetFor{For: to.For}})
	}
	if !g.optionsEqual(from.Options, to.Options) {
		options := to.Options
		if options == nil {
			options = &ast.Options{}
		}
		if optionsValueFromName(from.Options, "retention_period") != nil && optionsValueFromName(to.Options, "retention_period") == nil {
			options.Records = append(options.Records, &ast.OptionsDef{
				Name:  &ast.Ident{Name: "retention_period"},
				Value: &ast.StringLiteral{Value: "1d"},
			})
		}
		if optionsValueFromName(from.Options, "value_capture_type") != nil && optionsValueFromName(to.Options, "value_capture_type") == nil {
			options.Records = append(options.Records, &ast.OptionsDef{
				Name:  &ast.Ident{Name: "value_capture_type"},
				Value: &ast.StringLiteral{Value: "OLD_AND_NEW_VALUES"},
			})
		}
		ddl.Append(&ast.AlterChangeStream{Name: to.Name, ChangeStreamAlteration: &ast.ChangeStreamSetOptions{Options: options}})
	}
	return ddl
}

func (g *Generator) generateDDLForDropChangeStream(changeStream *ChangeStream) DDL {
	ddl := DDL{}
	ddl.Append(&ast.DropChangeStream{Name: changeStream.Name})
	return ddl
}

func (g *Generator) findViewByName(views []*View, name string) (view *View, exists bool) {
	for _, v := range views {
		if identsToComparable(v.Name.Idents...) == name {
			view = v
			exists = true
			break
		}
	}
	return
}

func (g *Generator) generateDDLForReplaceView(view *View) DDL {
	ddl := DDL{}
	ddl.Append(&ast.CreateView{Name: view.Name, Query: view.Query, SecurityType: ast.SecurityTypeInvoker, OrReplace: true})
	return ddl
}

func (g *Generator) generateDDLForDropView(view *View) DDL {
	ddl := DDL{}
	ddl.Append(&ast.DropView{Name: view.Name})
	return ddl
}
