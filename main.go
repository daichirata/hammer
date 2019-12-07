package main

import (
	"fmt"

	"cloud.google.com/go/spanner/spansql"
)

func main() {
	sql1 := `
CREATE TABLE users (
  user_id        STRING(36) NOT NULL,
  age            STRING(MAX) NOT NULL,
  created_at     TIMESTAMP NOT NULL,
  updated_at     TIMESTAMP NOT NULL,
) PRIMARY KEY (user_id);

CREATE TABLE friendships (
  user_id        STRING(36) NOT NULL,
  target_user_id STRING(36) NOT NULL,
  created_at     TIMESTAMP NOT NULL,
  updated_at     TIMESTAMP NOT NULL,
) PRIMARY KEY (user_id, target_user_id),
INTERLEAVE IN PARENT users ON DELETE CASCADE;
`
	sql2 := `
CREATE TABLE users (
  user_id        STRING(36) NOT NULL,
  name           STRING(36) NOT NULL,
  age            INT64,
  created_at     TIMESTAMP NOT NULL,
  updated_at     TIMESTAMP NOT NULL,
) PRIMARY KEY (user_id);

CREATE TABLE friendships (
  user_id        STRING(36) NOT NULL,
  target_user_id STRING(36) NOT NULL,
  friendship_id  STRING(36) NOT NULL,
  created_at     TIMESTAMP NOT NULL,
  updated_at     TIMESTAMP NOT NULL,
) PRIMARY KEY (user_id, target_user_id),
INTERLEAVE IN PARENT users ON DELETE CASCADE;
`
	ddl1, err := spansql.ParseDDL(sql1)
	if err != nil {
		panic(err)
	}
	from, err := NewDatabase(ddl1)
	if err != nil {
		panic(err)
	}

	ddl2, err := spansql.ParseDDL(sql2)
	if err != nil {
		panic(err)
	}
	to, err := NewDatabase(ddl2)
	if err != nil {
		panic(err)
	}
	generateDDLs(from, to)
}

type Database struct {
	Tables  []spansql.CreateTable
	Indexes []spansql.CreateIndex
}

func NewDatabase(ddl spansql.DDL) (*Database, error) {
	db := Database{}

	for _, istmt := range ddl.List {
		switch stmt := istmt.(type) {
		case spansql.CreateTable:
			db.Tables = append(db.Tables, stmt)
			break
		case spansql.CreateIndex:
			db.Indexes = append(db.Indexes, stmt)
			break
		default:
			return nil, fmt.Errorf("unexpected ddl type: %v", stmt)
		}
	}
	return &db, nil
}

func (d *Database) Index(name string) *spansql.CreateIndex {
	for _, i := range d.Indexes {
		if i.Name == name {
			return &i
		}
	}
	return nil
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

func generateDDLsForPrimryKey(from, to spansql.CreateTable) []string {
	ddls := []string{}
	for _, toPK := range to.PrimaryKey {
		fromPK, exists := findPrimryKeyByColumn(from.PrimaryKey, toPK.Column)
		if !exists || fromPK.Desc != toPK.Desc {
			ddls = append(ddls, spansql.DropTable{Name: from.Name}.SQL())
			ddls = append(ddls, to.SQL())
			return ddls
		}
	}
	for _, fromPK := range from.PrimaryKey {
		toPK, exists := findPrimryKeyByColumn(to.PrimaryKey, fromPK.Column)
		if !exists || fromPK.Desc != toPK.Desc {
			ddls = append(ddls, spansql.DropTable{Name: from.Name}.SQL())
			ddls = append(ddls, to.SQL())
			return ddls
		}
	}
	return ddls
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

func sameType(x, y spansql.ColumnDef) bool {
	return x.Type.Base == y.Type.Base && x.Type.Array == y.Type.Array
}

func allowNull(col spansql.ColumnDef) spansql.ColumnDef {
	col.NotNull = false
	return col
}

func generateDDLsForColumns(from, to spansql.CreateTable) []string {
	ddls := []string{}

	for _, toCol := range to.Columns {
		fromCol, exists := findColumnByName(from.Columns, toCol.Name)

		if exists {
			if fromCol == toCol {
				continue
			}

			if sameType(fromCol, toCol) {
				if !fromCol.NotNull && toCol.NotNull {
					ddls = append(ddls, UpdateColumn{TableName: to.Name, Def: toCol}.SQL())
				}
				ddls = append(ddls, AlterColumn{TableName: to.Name, Def: toCol}.SQL())
			} else {
				ddls = append(ddls, spansql.AlterTable{Name: from.Name, Alteration: spansql.DropColumn{Name: fromCol.Name}}.SQL())
				if toCol.NotNull {
					ddls = append(ddls, spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: allowNull(toCol)}}.SQL())
					ddls = append(ddls, UpdateColumn{TableName: to.Name, Def: toCol}.SQL())
					ddls = append(ddls, AlterColumn{TableName: to.Name, Def: toCol}.SQL())
				} else {
					ddls = append(ddls, spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: toCol}}.SQL())
				}
			}
		} else {
			if toCol.NotNull {
				ddls = append(ddls, spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: allowNull(toCol)}}.SQL())
				ddls = append(ddls, UpdateColumn{TableName: to.Name, Def: toCol}.SQL())
				ddls = append(ddls, AlterColumn{TableName: to.Name, Def: toCol}.SQL())
			} else {
				ddls = append(ddls, spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: toCol}}.SQL())
			}
		}
	}

	for _, fromCol := range from.Columns {
		if _, exists := findColumnByName(to.Columns, fromCol.Name); !exists {
			ddls = append(ddls, spansql.AlterTable{
				Name:       from.Name,
				Alteration: spansql.DropColumn{Name: fromCol.Name},
			}.SQL())
		}
	}
	return ddls
}

func findTableByName(tables []spansql.CreateTable, name string) (table spansql.CreateTable, exists bool) {
	for _, t := range tables {
		if t.Name == name {
			table = t
			exists = true
			break
		}
	}
	return
}

func generateDDLs(from, to *Database) {
	ddls := []string{}
	for _, toTable := range to.Tables {
		fromTable, exists := findTableByName(from.Tables, toTable.Name)
		if !exists {
			ddls = append(ddls, toTable.SQL())
			continue
		}

		if pkddls := generateDDLsForPrimryKey(fromTable, toTable); len(pkddls) > 0 {
			ddls = append(ddls, pkddls...)
		} else {
			ddls = append(ddls, generateDDLsForColumns(fromTable, toTable)...)
		}
	}
	for _, d := range ddls {
		fmt.Println(d)
	}
}
