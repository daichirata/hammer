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
		return nil, err
	}
	return NewDatabase(ddl)
}

type Database struct {
	tables []*Table
}

type Table struct {
	spansql.CreateTable

	indexes []spansql.CreateIndex
}

func NewDatabase(ddl spansql.DDL) (*Database, error) {
	table := map[string]*Table{}
	for _, istmt := range ddl.List {
		switch stmt := istmt.(type) {
		case spansql.CreateTable:
			if t, ok := table[stmt.Name]; ok {
				table[stmt.Name] = &Table{CreateTable: stmt, indexes: t.indexes}
			} else {
				table[stmt.Name] = &Table{CreateTable: stmt}
			}
			break
		case spansql.CreateIndex:
			if t, ok := table[stmt.Table]; ok {
				t.indexes = append(t.indexes, stmt)
			} else {
				table[stmt.Name] = &Table{indexes: []spansql.CreateIndex{stmt}}
			}
			break
		default:
			return nil, fmt.Errorf("unexpected ddl type: %v", stmt)
		}
	}
	tables := []*Table{}
	for _, v := range table {
		tables = append(tables, v)
	}
	return &Database{tables: tables}, nil
}

type Generator struct {
	from *Database
	to   *Database
}

func (g *Generator) GenerateDDLs() []DDL {
	ddls := []DDL{}

	for _, toTable := range g.to.tables {
		fromTable, exists := findTableByName(g.from.tables, toTable.Name)

		if exists {
			ddls = append(ddls, g.generateDDLsForDropIndex(fromTable.indexes, toTable.indexes)...)

			if pkddls := g.generateDDLsForPrimryKey(fromTable.CreateTable, toTable.CreateTable); len(pkddls) > 0 {
				ddls = append(ddls, pkddls...)
			} else {
				ddls = append(ddls, g.generateDDLsForColumns(fromTable.CreateTable, toTable.CreateTable)...)
			}
		} else {
			ddls = append(ddls, toTable)
		}
		ddls = append(ddls, g.generateDDLsForCreateIndex(fromTable.indexes, toTable.indexes)...)
	}
	return ddls
}

func (g *Generator) generateDDLsForDropIndex(from, to []spansql.CreateIndex) []DDL {
	ddls := []DDL{}
	for _, toIndex := range to {
		fromIndex, exists := findIndexByName(from, toIndex.Name)

		if exists && !reflect.DeepEqual(fromIndex, toIndex) {
			ddls = append(ddls, spansql.DropIndex{Name: fromIndex.Name})
		}
	}
	for _, fromIndex := range from {
		if _, exists := findIndexByName(to, fromIndex.Name); !exists {
			ddls = append(ddls, spansql.DropIndex{Name: fromIndex.Name})
		}
	}
	return ddls
}

func (g *Generator) generateDDLsForPrimryKey(from, to spansql.CreateTable) []DDL {
	ddls := []DDL{}
	for _, toPK := range to.PrimaryKey {
		fromPK, exists := findPrimryKeyByColumn(from.PrimaryKey, toPK.Column)

		if !exists || fromPK.Desc != toPK.Desc {
			ddls = append(ddls, spansql.DropTable{Name: from.Name})
			ddls = append(ddls, to)
			return ddls
		}
	}
	for _, fromPK := range from.PrimaryKey {
		toPK, exists := findPrimryKeyByColumn(to.PrimaryKey, fromPK.Column)

		if !exists || fromPK.Desc != toPK.Desc {
			ddls = append(ddls, spansql.DropTable{Name: from.Name})
			ddls = append(ddls, to)
			return ddls
		}
	}
	return ddls
}

func (g *Generator) generateDDLsForColumns(from, to spansql.CreateTable) []DDL {
	ddls := []DDL{}

	for _, toCol := range to.Columns {
		fromCol, exists := findColumnByName(from.Columns, toCol.Name)

		if exists {
			if reflect.DeepEqual(fromCol, toCol) {
				continue
			}

			if typeEqual(fromCol, toCol) {
				if !fromCol.NotNull && toCol.NotNull {
					ddls = append(ddls, Update{Table: to.Name, Def: toCol})
				}
				ddls = append(ddls, AlterColumn{Table: to.Name, Def: toCol})
			} else {
				ddls = append(ddls, spansql.AlterTable{Name: from.Name, Alteration: spansql.DropColumn{Name: fromCol.Name}})
				if toCol.NotNull {
					ddls = append(ddls, spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: allowNull(toCol)}})
					ddls = append(ddls, Update{Table: to.Name, Def: toCol})
					ddls = append(ddls, AlterColumn{Table: to.Name, Def: toCol})
				} else {
					ddls = append(ddls, spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: toCol}})
				}
			}
		} else {
			if toCol.NotNull {
				ddls = append(ddls, spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: allowNull(toCol)}})
				ddls = append(ddls, Update{Table: to.Name, Def: toCol})
				ddls = append(ddls, AlterColumn{Table: to.Name, Def: toCol})
			} else {
				ddls = append(ddls, spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: toCol}})
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

func (g *Generator) generateDDLsForCreateIndex(from, to []spansql.CreateIndex) []DDL {
	ddls := []DDL{}
	for _, toIndex := range to {
		fromIndex, exists := findIndexByName(from, toIndex.Name)

		if !exists || !reflect.DeepEqual(fromIndex, toIndex) {
			ddls = append(ddls, toIndex)
		}

	}
	return ddls
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
