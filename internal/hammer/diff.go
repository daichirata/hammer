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

	m := make(map[spansql.ID]*Table)
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

	dropedTable []spansql.ID
	dropedIndex []spansql.ID
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
			ddl.AppendDDL(g.generateDDLForDropIndexAndTable(fromTable))
			ddl.AppendDDL(g.generateDDLForCreateTableAndIndex(toTable))
			continue
		}

		if !g.primaryKeyEqual(fromTable, toTable) {
			ddl.AppendDDL(g.generateDDLForDropIndexAndTable(fromTable))
			ddl.AppendDDL(g.generateDDLForCreateTableAndIndex(toTable))
			continue
		}

		ddl.AppendDDL(g.generateDDLForDropIndex(fromTable, toTable))
		ddl.AppendDDL(g.generateDDLForColumns(fromTable, toTable))
		ddl.AppendDDL(g.generateDDLForCreateIndex(fromTable, toTable))
	}
	for _, fromTable := range g.from.tables {
		if _, exists := g.findTableByName(g.to.tables, fromTable.Name); !exists {
			ddl.AppendDDL(g.generateDDLForDropIndexAndTable(fromTable))
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

func (g *Generator) generateDDLForDropIndexAndTable(table *Table) DDL {
	ddl := DDL{}

	if g.isDropedTable(table.Name) {
		return ddl
	}
	for _, t := range table.children {
		ddl.AppendDDL(g.generateDDLForDropIndexAndTable(t))
	}
	for _, i := range table.indexes {
		ddl.Append(spansql.DropIndex{Name: i.Name})
	}
	ddl.Append(spansql.DropTable{Name: table.Name})
	g.dropedTable = append(g.dropedTable, table.Name)
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
			ddl.Append(spansql.AlterTable{
				Name:       from.Name,
				Alteration: spansql.DropColumn{Name: fromCol.Name},
			})
		}
	}
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
	ddl.Append(spansql.AlterTable{Name: from.Name, Alteration: spansql.DropColumn{Name: fromCol.Name}})
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

func (g *Generator) isDropedTable(name spansql.ID) bool {
	for _, t := range g.dropedTable {
		if t == name {
			return true
		}
	}
	return false
}

func (g *Generator) isDropedIndex(name spansql.ID) bool {
	for _, t := range g.dropedIndex {
		if t == name {
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

func (g *Generator) indexEqual(x, y spansql.CreateIndex) bool {
	x.Position = spansql.Position{}
	y.Position = spansql.Position{}

	return reflect.DeepEqual(x, y)
}

func (g *Generator) allowNull(col spansql.ColumnDef) spansql.ColumnDef {
	col.NotNull = false
	return col
}

func (g *Generator) findTableByName(tables []*Table, name spansql.ID) (table *Table, exists bool) {
	for _, t := range tables {
		if t.Name == name {
			table = t
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findColumnByName(cols []spansql.ColumnDef, name spansql.ID) (col spansql.ColumnDef, exists bool) {
	for _, c := range cols {
		if c.Name == name {
			col = c
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findIndexByName(indexes []*spansql.CreateIndex, name spansql.ID) (index *spansql.CreateIndex, exists bool) {
	for _, i := range indexes {
		if i.Name == name {
			index = i
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findIndexByColumn(indexes []*spansql.CreateIndex, column spansql.ID) []*spansql.CreateIndex {
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
