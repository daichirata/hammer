package main

import (
	"fmt"

	"cloud.google.com/go/spanner/spansql"
)

func main() {
	sql1 := `
CREATE TABLE users (
  user_id        STRING(36) NOT NULL,
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

func findPrimryKeyByColumn(keys []spansql.KeyPart, column string) *spansql.KeyPart {
	for _, key := range keys {
		if key.Column == column {
			return &key
		}
	}
	return nil
}

func generateDDLsForPrimryKey(from, to spansql.CreateTable) []string {
	ddls := []string{}
	for _, toPK := range to.PrimaryKey {
		fromPK := findPrimryKeyByColumn(from.PrimaryKey, toPK.Column)
		if fromPK == nil || fromPK.Desc != toPK.Desc {
			ddls = append(ddls, spansql.DropTable{Name: from.Name}.SQL())
			ddls = append(ddls, to.SQL())
			return ddls
		}
	}
	for _, fromPK := range from.PrimaryKey {
		toPK := findPrimryKeyByColumn(to.PrimaryKey, fromPK.Column)
		if toPK == nil || fromPK.Desc != toPK.Desc {
			ddls = append(ddls, spansql.DropTable{Name: from.Name}.SQL())
			ddls = append(ddls, to.SQL())
			return ddls
		}
	}
	return ddls
}

func findColumnByName(cols []spansql.ColumnDef, name string) (col spansql.ColumnDef) {
	for _, c := range cols {
		if c.Name == name {
			col = c
			break
		}
	}
	return
}

func defaultValue(base spansql.TypeBase) string {
	switch base {
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

func changeableColumn(col spansql.ColumnDef) bool {
	return (col.Type.Base == spansql.String || col.Type.Base == spansql.Bytes) && !col.Type.Array
}

func generateDDLsForColumns(from, to spansql.CreateTable) []string {
	ddls := []string{}

	for _, toCol := range to.Columns {
		fromCol := findColumnByName(from.Columns, toCol.Name)
		if fromCol == toCol {
			continue
		}
		exists := fromCol.Name != ""

		if exists && !(changeableColumn(fromCol) && changeableColumn(toCol)) {
			ddls = append(ddls, spansql.AlterTable{
				Name:       from.Name,
				Alteration: spansql.DropColumn{Name: fromCol.Name},
			}.SQL())
		}
		if toCol.NotNull && !(exists && fromCol.NotNull) {
			allowNull := toCol
			allowNull.NotNull = false
			ddls = append(ddls, spansql.AlterTable{
				Name:       to.Name,
				Alteration: spansql.AddColumn{Def: allowNull},
			}.SQL())
			ddls = append(ddls, fmt.Sprintf("UPDATE %s SET %s = %s", toCol.Name, toCol.Name, defaultValue(toCol.Type.Base)))
		}
		ddls = append(ddls, spansql.AlterTable{
			Name:       to.Name,
			Alteration: spansql.AddColumn{Def: toCol},
		}.SQL())
	}

	for _, fromCol := range from.Columns {
		toCol := findColumnByName(to.Columns, fromCol.Name)
		if fromCol == toCol {
			continue
		}
		exists := toCol.Name != ""

		if !exists {
			ddls = append(ddls, spansql.AlterTable{
				Name:       from.Name,
				Alteration: spansql.DropColumn{Name: fromCol.Name},
			}.SQL())
		}
	}
	return ddls
}

func findTableByName(tables []spansql.CreateTable, name string) (table spansql.CreateTable) {
	for _, t := range tables {
		if t.Name == name {
			table = t
			break
		}
	}
	return
}

func generateDDLs(from, to *Database) {
	ddls := []string{}
	for _, toTable := range to.Tables {
		fromTable := findTableByName(from.Tables, toTable.Name)
		if fromTable.Name == "" {
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
