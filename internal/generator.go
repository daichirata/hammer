package internal

import (
	"fmt"
	"reflect"

	"cloud.google.com/go/spanner/spansql"
)

func GenerateDDLs(from, to Source) ([]DDL, error) {
	fromDB, err := parseDDL(from)
	if err != nil {
		return nil, err
	}
	toDB, err := parseDDL(to)
	if err != nil {
		return nil, err
	}

	generator := &Generator{
		from: fromDB,
		to:   toDB,
	}
	return generator.GenerateDDLs(), nil
}

func parseDDL(source Source) (*Database, error) {
	schema, err := source.Read()
	if err != nil {
		return nil, err
	}
	ddl, err := spansql.ParseDDL(schema)
	if err != nil {
		return nil, fmt.Errorf("%s failed to parse ddl: %w", source, err)
	}
	return NewDatabase(ddl)
}

type Database struct {
	tables []*Table
}

type Table struct {
	spansql.CreateTable

	indexes  []spansql.CreateIndex
	children []*Table
}

func NewDatabase(ddl spansql.DDL) (*Database, error) {
	tables := []*Table{}
	tmap := map[string]*Table{}
	for _, istmt := range ddl.List {
		switch stmt := istmt.(type) {
		case spansql.CreateTable:
			table := &Table{CreateTable: stmt}
			tmap[stmt.Name] = table
			tables = append(tables, table)
			break
		case spansql.CreateIndex:
			if t, ok := tmap[stmt.Table]; ok {
				t.indexes = append(t.indexes, stmt)
			} else {
				return nil, fmt.Errorf("cannot find ddl of table to apply index %s", stmt.Name)
			}
			break
		default:
			return nil, fmt.Errorf("unexpected ddl type: %v", stmt)
		}
	}
	for _, t := range tmap {
		if i := t.Interleave; i != nil {
			if p, ok := tmap[i.Parent]; ok {
				p.children = append(p.children, t)
			} else {
				return nil, fmt.Errorf("parent ddl %s not found", i.Parent)
			}
		}
	}
	return &Database{tables: tables}, nil
}

type Generator struct {
	from *Database
	to   *Database

	dropedTable []string
	dropedIndex []string
}

func (g *Generator) GenerateDDLs() []DDL {
	ddls := []DDL{}
	for _, toTable := range g.to.tables {
		fromTable, exists := findTableByName(g.from.tables, toTable.Name)

		if !exists {
			ddls = append(ddls, toTable)
			for _, i := range toTable.indexes {
				ddls = append(ddls, i)
			}
			continue
		}

		if g.isDropedTable(toTable.Name) {
			ddls = append(ddls, g.generateDDLsForCreateTableAndIndex(toTable)...)
			continue
		}

		if !reflect.DeepEqual(fromTable.Interleave, toTable.Interleave) {
			ddls = append(ddls, g.generateDDLsForDropIndexAndTable(fromTable)...)
			ddls = append(ddls, g.generateDDLsForCreateTableAndIndex(toTable)...)
			continue
		}

		if !g.primaryKeyDeepEqual(fromTable, toTable) {
			ddls = append(ddls, g.generateDDLsForDropIndexAndTable(fromTable)...)
			ddls = append(ddls, g.generateDDLsForCreateTableAndIndex(toTable)...)
			continue
		}

		ddls = append(ddls, g.generateDDLsForDropIndex(fromTable, toTable)...)
		ddls = append(ddls, g.generateDDLsForColumns(fromTable, toTable)...)
		ddls = append(ddls, g.generateDDLsForCreateIndex(fromTable, toTable)...)
	}
	for _, fromTable := range g.from.tables {
		if _, exists := findTableByName(g.to.tables, fromTable.Name); !exists {
			ddls = append(ddls, g.generateDDLsForDropIndexAndTable(fromTable)...)
		}
	}
	return ddls
}

func (g *Generator) generateDDLsForDropIndexAndTable(table *Table) []DDL {
	ddls := []DDL{}
	if g.isDropedTable(table.Name) {
		return ddls
	}
	for _, t := range table.children {
		ddls = append(ddls, g.generateDDLsForDropIndexAndTable(t)...)
	}
	for _, i := range table.indexes {
		ddls = append(ddls, spansql.DropIndex{Name: i.Name})
	}
	ddls = append(ddls, spansql.DropTable{Name: table.Name})
	g.dropedTable = append(g.dropedTable, table.Name)
	return ddls
}

func (g *Generator) generateDDLsForCreateTableAndIndex(table *Table) []DDL {
	ddls := []DDL{}
	ddls = append(ddls, table)
	for _, i := range table.indexes {
		ddls = append(ddls, i)
	}
	return ddls
}

func (g *Generator) primaryKeyDeepEqual(from, to *Table) bool {
	if !reflect.DeepEqual(from.PrimaryKey, to.PrimaryKey) {
		return false
	}
	for _, pk := range to.PrimaryKey {
		fromCol, exists := findColumnByName(from.Columns, pk.Column)
		if !exists {
			return false
		}
		toCol, exists := findColumnByName(to.Columns, pk.Column)
		if !exists {
			return false
		}
		if !reflect.DeepEqual(fromCol, toCol) {
			return false
		}
	}
	return true
}

func (g *Generator) generateDDLsForColumns(from, to *Table) []DDL {
	ddls := []DDL{}
	for _, toCol := range to.Columns {
		fromCol, exists := findColumnByName(from.Columns, toCol.Name)

		if !exists {
			if toCol.NotNull {
				ddls = append(ddls, spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: allowNull(toCol)}})
				ddls = append(ddls, Update{Table: to.Name, Def: toCol})
				ddls = append(ddls, AlterColumn{Table: to.Name, Def: toCol})
			} else {
				ddls = append(ddls, spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: toCol}})
			}
			continue
		}

		if reflect.DeepEqual(fromCol, toCol) {
			continue
		}

		if typeEqual(fromCol, toCol) {
			if !fromCol.NotNull && toCol.NotNull {
				ddls = append(ddls, Update{Table: to.Name, Def: toCol})
			}
			ddls = append(ddls, AlterColumn{Table: to.Name, Def: toCol})
		} else {
			indexes := []spansql.CreateIndex{}
			for _, i := range findIndexByColumn(from.indexes, fromCol.Name) {
				if !g.isDropedIndex(i.Name) {
					indexes = append(indexes, i)
				}
			}
			for _, i := range indexes {
				ddls = append(ddls, spansql.DropIndex{Name: i.Name})
			}
			ddls = append(ddls, spansql.AlterTable{Name: from.Name, Alteration: spansql.DropColumn{Name: fromCol.Name}})
			if toCol.NotNull {
				ddls = append(ddls, spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: allowNull(toCol)}})
				ddls = append(ddls, Update{Table: to.Name, Def: toCol})
				ddls = append(ddls, AlterColumn{Table: to.Name, Def: toCol})
			} else {
				ddls = append(ddls, spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: toCol}})
			}
			for _, i := range indexes {
				ddls = append(ddls, i)
			}
		}
	}
	for _, fromCol := range from.Columns {
		if _, exists := findColumnByName(to.Columns, fromCol.Name); !exists {
			ddls = append(ddls, spansql.AlterTable{
				Name:       from.Name,
				Alteration: spansql.DropColumn{Name: fromCol.Name},
			})
		}
	}
	return ddls
}

func (g *Generator) generateDDLsForDropIndex(from, to *Table) []DDL {
	ddls := []DDL{}
	for _, toIndex := range to.indexes {
		fromIndex, exists := findIndexByName(from.indexes, toIndex.Name)

		if exists && !reflect.DeepEqual(fromIndex, toIndex) {
			ddls = append(ddls, spansql.DropIndex{Name: fromIndex.Name})
			g.dropedIndex = append(g.dropedIndex, fromIndex.Name)
		}
	}
	for _, fromIndex := range from.indexes {
		if _, exists := findIndexByName(to.indexes, fromIndex.Name); !exists {
			ddls = append(ddls, spansql.DropIndex{Name: fromIndex.Name})
			g.dropedIndex = append(g.dropedIndex, fromIndex.Name)
		}
	}
	return ddls
}

func (g *Generator) generateDDLsForCreateIndex(from, to *Table) []DDL {
	ddls := []DDL{}
	for _, toIndex := range to.indexes {
		fromIndex, exists := findIndexByName(from.indexes, toIndex.Name)

		if !exists || !reflect.DeepEqual(fromIndex, toIndex) {
			ddls = append(ddls, toIndex)
		}

	}
	return ddls
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

func typeEqual(x, y spansql.ColumnDef) bool {
	return x.Type.Base == y.Type.Base && x.Type.Array == y.Type.Array
}

func allowNull(col spansql.ColumnDef) spansql.ColumnDef {
	col.NotNull = false
	return col
}

func findTableByName(tables []*Table, name string) (table *Table, exists bool) {
	for _, t := range tables {
		if t.Name == name {
			table = t
			exists = true
			break
		}
	}
	return
}

func findPrimryKeyByColumn(keys []spansql.KeyPart, column string) (key spansql.KeyPart, exists bool) {
	for _, k := range keys {
		if k.Column == column {
			key = k
			exists = true
			break
		}
	}
	return
}

func findColumnByName(cols []spansql.ColumnDef, name string) (col spansql.ColumnDef, exists bool) {
	for _, c := range cols {
		if c.Name == name {
			col = c
			exists = true
			break
		}
	}
	return
}

func findIndexByName(indexes []spansql.CreateIndex, name string) (index spansql.CreateIndex, exists bool) {
	for _, i := range indexes {
		if i.Name == name {
			index = i
			exists = true
			break
		}
	}
	return
}

func findIndexByColumn(indexes []spansql.CreateIndex, column string) []spansql.CreateIndex {
	result := []spansql.CreateIndex{}
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
