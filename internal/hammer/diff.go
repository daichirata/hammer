package hammer

import (
	"fmt"
	"reflect"

	"cloud.google.com/go/spanner/spansql"
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
		from: database1,
		to:   database2,
	}
	return generator.GenerateDDL(), nil
}

func NewDatabase(ddl DDL) (*Database, error) {
	var tables []*Table

	m := make(map[string]*Table)
	for _, istmt := range ddl.List {
		switch stmt := istmt.(type) {
		case *spansql.CreateTable:
			t := &Table{CreateTable: stmt}
			tables = append(tables, t)
			m[stmt.Name] = t
		case *spansql.CreateIndex:
			if t, ok := m[stmt.Table]; ok {
				t.indexes = append(t.indexes, stmt)
			} else {
				return nil, fmt.Errorf("cannot find ddl of table to apply index %s", stmt.Name)
			}
		case *spansql.AlterTable:
			t, ok := m[stmt.Name]
			if !ok {
				return nil, fmt.Errorf("cannot find ddl of table to apply index %s", stmt.Name)
			}
			switch alteration := stmt.Alteration.(type) {
			case spansql.AddConstraint:
				t.Constraints = append(t.Constraints, alteration.Constraint)
			default:
				return nil, fmt.Errorf("unsupported table alteration: %v", stmt)
			}
		default:
			return nil, fmt.Errorf("unexpected ddl statement: %v", stmt)
		}
	}
	for _, t := range tables {
		if i := t.Interleave; i != nil {
			if p, ok := m[i.Parent]; ok {
				p.children = append(p.children, t)
			} else {
				return nil, fmt.Errorf("parent ddl %s not found", i.Parent)
			}
		}
	}

	return &Database{tables: tables}, nil
}

type Database struct {
	tables []*Table
}

type Table struct {
	*spansql.CreateTable

	indexes  []*spansql.CreateIndex
	children []*Table
}

type Generator struct {
	from *Database
	to   *Database

	dropedTable        []string
	dropedIndex        []string
	droppedConstraints []spansql.TableConstraint
}

func (g *Generator) GenerateDDL() DDL {
	ddl := DDL{}

	for _, toTable := range g.to.tables {
		fromTable, exists := g.findTableByName(g.from.tables, toTable.Name)

		if !exists {
			ddl.AppendDDL(g.generateDDLForCreateTableAndIndex(toTable))
			continue
		}

		if g.isDropedTable(toTable.Name) {
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
	}
	for _, fromTable := range g.from.tables {
		if _, exists := g.findTableByName(g.to.tables, fromTable.Name); !exists {
			ddl.AppendDDL(g.generateDDLForDropConstraintIndexAndTable(fromTable))
		}
	}
	return ddl
}

func (g *Generator) generateDDLForCreateTableAndIndex(table *Table) DDL {
	ddl := DDL{}

	ddl.Append(table)
	for _, i := range table.indexes {
		ddl.Append(i)
	}
	return ddl
}

func (g *Generator) generateDDLForDropConstraintIndexAndTable(table *Table) DDL {
	ddl := DDL{}

	if g.isDropedTable(table.Name) {
		return ddl
	}
	for _, t := range table.children {
		ddl.AppendDDL(g.generateDDLForDropConstraintIndexAndTable(t))
	}
	for _, i := range table.indexes {
		ddl.Append(spansql.DropIndex{Name: i.Name})
	}
	ddl.AppendDDL(g.generateDDLForDropNamedConstraintsMatchingPredicate(func(constraint spansql.TableConstraint) bool {
		return constraint.ForeignKey.RefTable == table.Name
	}))
	ddl.Append(spansql.DropTable{Name: table.Name})
	g.dropedTable = append(g.dropedTable, table.Name)
	return ddl
}

func (g *Generator) generateDDLForConstraints(from, to *Table) DDL {
	ddl := DDL{}

	for _, toConstraint := range to.Constraints {
		isUnnamedConstraint := toConstraint.Name == ""

		if isUnnamedConstraint {
			_, exists := g.findUnnamedConstraint(from.Constraints, toConstraint)
			if !exists {
				ddl.Append(spansql.AlterTable{Name: to.Name, Alteration: spansql.AddConstraint{Constraint: toConstraint}})
			}
			continue
		}

		fromConstraint, exists := g.findNamedConstraint(from.Constraints, toConstraint.Name)

		if !exists || g.isDroppedConstraint(toConstraint) {
			ddl.Append(spansql.AlterTable{Name: to.Name, Alteration: spansql.AddConstraint{Constraint: toConstraint}})
			continue
		}

		if g.constraintEqual(fromConstraint, toConstraint) {
			continue
		}

		ddl.AppendDDL(g.generateDDLForDropNamedConstraint(from.Name, fromConstraint))
		ddl.Append(spansql.AlterTable{
			Name:       to.Name,
			Alteration: spansql.AddConstraint{Constraint: toConstraint},
		})
	}

	for _, fromConstraint := range from.Constraints {
		if fromConstraint.Name == "" {
			continue
		}

		if _, exists := g.findNamedConstraint(to.Constraints, fromConstraint.Name); !exists {
			ddl.AppendDDL(g.generateDDLForDropNamedConstraint(from.Name, fromConstraint))
		}
	}
	return ddl
}

func (g *Generator) generateDDLForColumns(from, to *Table) DDL {
	ddl := DDL{}

	for _, toCol := range to.Columns {
		fromCol, exists := g.findColumnByName(from.Columns, toCol.Name)

		if !exists {
			if toCol.NotNull {
				ddl.Append(spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: g.allowNull(toCol)}})
				ddl.Append(Update{Table: to.Name, Def: toCol})
				ddl.Append(AlterColumn{Table: to.Name, Def: toCol})
			} else {
				ddl.Append(spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: toCol}})
			}
			continue
		}

		if g.columnDefEqual(fromCol, toCol) {
			continue
		}

		if g.columnTypeEqual(fromCol, toCol) {
			if fromCol.Type.Base == spansql.Timestamp {
				if fromCol.NotNull != toCol.NotNull {
					if !fromCol.NotNull && toCol.NotNull {
						ddl.Append(Update{Table: to.Name, Def: toCol})
					}
					ddl.Append(AlterColumn{Table: to.Name, Def: toCol})
				}
				if !reflect.DeepEqual(fromCol.Options.AllowCommitTimestamp, toCol.Options.AllowCommitTimestamp) {
					ddl.Append(AlterColumn{Table: to.Name, Def: toCol, SetOptions: true})
				}
			} else {
				if !fromCol.NotNull && toCol.NotNull {
					ddl.Append(Update{Table: to.Name, Def: toCol})
				}
				ddl.Append(AlterColumn{Table: to.Name, Def: toCol})
			}
		} else {
			ddl.AppendDDL(g.generateDDLForDropAndCreateColumn(from, to, fromCol, toCol))
		}
	}
	for _, fromCol := range from.Columns {
		if _, exists := g.findColumnByName(to.Columns, fromCol.Name); !exists {
			ddl.AppendDDL(g.generateDDLForDropColumn(from.Name, fromCol.Name))
		}
	}
	return ddl
}

func (g *Generator) generateDDLForDropColumn(table string, column string) DDL {
	ddl := DDL{}

	ddl.AppendDDL(g.generateDDLForDropNamedConstraintsMatchingPredicate(func(constraint spansql.TableConstraint) bool {
		for _, c := range constraint.ForeignKey.Columns {
			if column == c {
				return true
			}
		}

		for _, refColumn := range constraint.ForeignKey.RefColumns {
			if column == refColumn {
				return true
			}
		}

		return false
	}))

	ddl.Append(spansql.AlterTable{
		Name:       table,
		Alteration: spansql.DropColumn{Name: column},
	})

	return ddl
}

func (g *Generator) generateDDLForDropAndCreateColumn(from, to *Table, fromCol, toCol spansql.ColumnDef) DDL {
	ddl := DDL{}

	indexes := []*spansql.CreateIndex{}
	for _, i := range g.findIndexByColumn(from.indexes, fromCol.Name) {
		if !g.isDropedIndex(i.Name) {
			indexes = append(indexes, i)
		}
	}
	for _, i := range indexes {
		ddl.Append(spansql.DropIndex{Name: i.Name})
	}

	ddl.AppendDDL(g.generateDDLForDropColumn(from.Name, fromCol.Name))

	if toCol.NotNull {
		ddl.Append(spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: g.allowNull(toCol)}})
		ddl.Append(Update{Table: to.Name, Def: toCol})
		ddl.Append(AlterColumn{Table: to.Name, Def: toCol})
	} else {
		ddl.Append(spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: toCol}})
	}
	for _, i := range indexes {
		ddl.Append(i)
	}
	return ddl
}

func (g *Generator) generateDDLForDropIndex(from, to *Table) DDL {
	ddl := DDL{}

	for _, toIndex := range to.indexes {
		fromIndex, exists := g.findIndexByName(from.indexes, toIndex.Name)

		if exists && !g.indexEqual(*fromIndex, *toIndex) {
			ddl.Append(spansql.DropIndex{Name: fromIndex.Name})
			g.dropedIndex = append(g.dropedIndex, fromIndex.Name)
		}
	}
	for _, fromIndex := range from.indexes {
		if _, exists := g.findIndexByName(to.indexes, fromIndex.Name); !exists {
			ddl.Append(spansql.DropIndex{Name: fromIndex.Name})
			g.dropedIndex = append(g.dropedIndex, fromIndex.Name)
		}
	}
	return ddl
}

func (g *Generator) generateDDLForCreateIndex(from, to *Table) DDL {
	ddl := DDL{}

	for _, toIndex := range to.indexes {
		fromIndex, exists := g.findIndexByName(from.indexes, toIndex.Name)

		if !exists || !g.indexEqual(*fromIndex, *toIndex) {
			ddl.Append(toIndex)
		}
	}
	return ddl
}

func (g *Generator) generateDDLForDropNamedConstraint(table string, constraint spansql.TableConstraint) DDL {
	ddl := DDL{}

	if constraint.Name == "" {
		return ddl
	}

	for _, c := range g.droppedConstraints {
		if g.constraintEqual(c, constraint) {
			return ddl
		}
	}
	g.droppedConstraints = append(g.droppedConstraints, constraint)

	ddl.Append(spansql.AlterTable{
		Name:       table,
		Alteration: spansql.DropConstraint{Name: constraint.Name},
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

func (g *Generator) isDroppedConstraint(constraint spansql.TableConstraint) bool {
	for _, c := range g.droppedConstraints {
		if g.constraintEqual(c, constraint) {
			return true
		}
	}
	return false
}

func (g *Generator) interleaveEqual(x, y *Table) bool {
	return reflect.DeepEqual(x.Interleave, y.Interleave)
}

func (g *Generator) primaryKeyEqual(x, y *Table) bool {
	if !reflect.DeepEqual(x.PrimaryKey, y.PrimaryKey) {
		return false
	}
	for _, pk := range y.PrimaryKey {
		xCol, exists := g.findColumnByName(x.Columns, pk.Column)
		if !exists {
			return false
		}
		yCol, exists := g.findColumnByName(y.Columns, pk.Column)
		if !exists {
			return false
		}
		if !g.columnDefEqual(xCol, yCol) {
			return false
		}
	}
	return true
}

func (g *Generator) columnDefEqual(x, y spansql.ColumnDef) bool {
	x.Position = spansql.Position{}
	y.Position = spansql.Position{}

	return reflect.DeepEqual(x, y)
}

func (g *Generator) columnTypeEqual(x, y spansql.ColumnDef) bool {
	return x.Type.Base == y.Type.Base && x.Type.Array == y.Type.Array
}

func (g *Generator) constraintEqual(x, y spansql.TableConstraint) bool {
	x.Position = spansql.Position{}
	y.Position = spansql.Position{}
	x.ForeignKey.Position = spansql.Position{}
	y.ForeignKey.Position = spansql.Position{}

	return reflect.DeepEqual(x, y)
}

func (g *Generator) indexEqual(x, y spansql.CreateIndex) bool {
	x.Position = spansql.Position{}
	y.Position = spansql.Position{}

	return reflect.DeepEqual(x, y)
}

func (g *Generator) allowNull(col spansql.ColumnDef) spansql.ColumnDef {
	col.NotNull = false
	return col
}

func (g *Generator) findTableByName(tables []*Table, name string) (table *Table, exists bool) {
	for _, t := range tables {
		if t.Name == name {
			table = t
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findColumnByName(cols []spansql.ColumnDef, name string) (col spansql.ColumnDef, exists bool) {
	for _, c := range cols {
		if c.Name == name {
			col = c
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findNamedConstraint(constraints []spansql.TableConstraint, name string) (con spansql.TableConstraint, exists bool) {
	if name == "" {
		exists = false
		return
	}

	for _, c := range constraints {
		if c.Name == name {
			con = c
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findUnnamedConstraint(constraints []spansql.TableConstraint, item spansql.TableConstraint) (con spansql.TableConstraint, exists bool) {
	for _, c := range constraints {
		if g.constraintEqual(c, item) {
			con = c
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findIndexByName(indexes []*spansql.CreateIndex, name string) (index *spansql.CreateIndex, exists bool) {
	for _, i := range indexes {
		if i.Name == name {
			index = i
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findIndexByColumn(indexes []*spansql.CreateIndex, column string) []*spansql.CreateIndex {
	result := []*spansql.CreateIndex{}
	for _, i := range indexes {
		for _, c := range i.Columns {
			if c.Column == column {
				result = append(result, i)
				break
			}
		}
	}
	return result
}

func (g *Generator) generateDDLForDropNamedConstraintsMatchingPredicate(predicate func(spansql.TableConstraint) bool) DDL {
	ddl := DDL{}

	for _, table := range g.from.tables {
		for _, constraint := range table.Constraints {
			if predicate(constraint) {
				ddl.AppendDDL(g.generateDDLForDropNamedConstraint(table.Name, constraint))
			}
		}
	}

	return ddl
}
