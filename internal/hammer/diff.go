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
		alteredChangeStreamStates:        map[string]*ChangeStream{},
	}
	return generator.GenerateDDL(), nil
}

func NewDatabase(ddl DDL) (*Database, error) {
	var (
		tables               []*Table
		changeStreams        []*ChangeStream
		views                []*View
		roles                []*Role
		grants               []*Grant
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
		case *ast.CreateRole:
			roles = append(roles, &Role{CreateRole: stmt})
		case *ast.Grant:
			grants = append(grants, &Grant{Grant: stmt})
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

	return &Database{tables: tables, changeStreams: changeStreams, views: views, roles: roles, grants: grants, alterDatabaseOptions: alterDatabaseOptions, options: options}, nil
}

type Database struct {
	tables               []*Table
	changeStreams        []*ChangeStream
	views                []*View
	roles                []*Role
	grants               []*Grant
	alterDatabaseOptions *ast.AlterDatabase
	options              *ast.Options
}

func (d *Database) grantsOnTable(table *Table) []*Grant {
	var result []*Grant
	target := identsToComparable(table.Name.Idents...)

	for _, grant := range d.grants {
		switch p := grant.Grant.Privilege.(type) {
		case *ast.PrivilegeOnTable:
			for _, name := range p.Names {
				if identsToComparable(name) == target {
					result = append(result, grant)
					break
				}
			}
		}
	}
	return result
}

func (d *Database) grantsFromPath(path *ast.Path) []*Grant {
	var result []*Grant
	target := identsToComparable(path.Idents...)

	for _, grant := range d.grants {
		switch p := grant.Grant.Privilege.(type) {
		case *ast.PrivilegeOnTable:
			for _, name := range p.Names {
				if identsToComparable(name) == target {
					result = append(result, grant)
					break
				}
			}
		}
	}
	return result
}

func (d *Database) grantsOnView(view *View) []*Grant {
	var result []*Grant
	target := identsToComparable(view.Name.Idents...)
	for _, grant := range d.grants {
		if p, exists := grant.Grant.Privilege.(*ast.SelectPrivilegeOnView); exists {
			for _, name := range p.Names {
				if identsToComparable(name) == target {
					result = append(result, grant)
					break
				}
			}
		}
	}
	return result
}

func (d *Database) grantsOnChangeStream(cs *ChangeStream) []*Grant {
	var result []*Grant
	target := identsToComparable(cs.Name)

	for _, grant := range d.grants {
		if p, exists := grant.Grant.Privilege.(*ast.SelectPrivilegeOnChangeStream); exists {
			for _, name := range p.Names {
				if identsToComparable(name) == target {
					result = append(result, grant)
					break
				}
			}
		}
	}
	return result
}

func (d *Database) grantsOnRole(role *Role) []*Grant {
	var result []*Grant
	target := identsToComparable(role.Name)

	for _, grant := range d.grants {
		for _, r := range grant.Roles {
			if identsToComparable(r) == target {
				result = append(result, grant)
				break
			}
		}
	}
	return result
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

type Role struct {
	*ast.CreateRole
}

type Grant struct {
	*ast.Grant
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
	droppedGrant                     []*Grant
	willCreateOrAlterChangeStreamIDs map[string]*ChangeStream
	alteredChangeStreamStates        map[string]*ChangeStream
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
		ddl.AppendDDL(g.generateDDLForAlterIndex(fromTable, toTable))
		ddl.AppendDDL(g.generateDDLForConstraints(fromTable, toTable))
		ddl.AppendDDL(g.generateDDLForRowDeletionPolicy(fromTable, toTable))
		ddl.AppendDDL(g.generateDDLForCreateChangeStream(g.from, toTable))
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
	for _, cs := range g.willCreateOrAlterChangeStreamIDs {
		fromChangeStream, exists := g.findChangeStreamByName(g.from, identsToComparable(cs.Name))
		if !exists || g.isDropedChangeStream(identsToComparable(cs.Name)) {
			ddl.Append(cs)
			continue
		}
		if alteredState, hasAlteredState := g.alteredChangeStreamStates[identsToComparable(cs.Name)]; hasAlteredState {
			fromChangeStream = alteredState
		}
		ddl.AppendDDL(g.generateDDLForAlterChangeStream(fromChangeStream, cs))
	}
	// drop tables
	for _, fromTable := range g.from.tables {
		if _, exists := g.findTableByName(g.to.tables, identsToComparable(fromTable.Name.Idents...)); !exists {
			ddl.AppendDDL(g.generateDDLForDropConstraintIndexAndTable(fromTable))
		}
	}
	// drop change streams
	for _, fromChangeStream := range g.from.changeStreams {
		if g.isDropedChangeStream(identsToComparable(fromChangeStream.Name)) {
			continue
		}
		if _, exists := g.findChangeStreamByName(g.to, identsToComparable(fromChangeStream.Name)); !exists {
			ddl.AppendDDL(g.generateDDLForDropChangeStream(fromChangeStream))
			g.dropedChangeStream = append(g.dropedChangeStream, identsToComparable(fromChangeStream.Name))
		}
	}
	for _, fromTable := range g.from.tables {
		if _, exists := g.findTableByName(g.to.tables, identsToComparable(fromTable.Name.Idents...)); !exists {
			continue
		}
		for _, cs := range fromTable.changeStreams {
			if g.isDropedChangeStream(identsToComparable(cs.Name)) {
				continue
			}
			if _, exists := g.findChangeStreamByName(g.to, identsToComparable(cs.Name)); !exists {
				ddl.AppendDDL(g.generateDDLForDropChangeStream(cs))
				g.dropedChangeStream = append(g.dropedChangeStream, identsToComparable(cs.Name))
			}
		}
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

	// for roles
	for _, toRole := range g.to.roles {
		roleName := identsToComparable(toRole.Name)
		if _, exists := g.findRoleByName(g.from.roles, roleName); !exists {
			ddl.Append(toRole)
			continue
		}
	}
	for _, fromRole := range g.from.roles {
		roleName := identsToComparable(fromRole.Name)
		if _, exists := g.findRoleByName(g.to.roles, roleName); !exists {
			ddl.AppendDDL(g.generateDDLForDropRole(fromRole))
		}
	}

	// for grants
	for _, fromGrant := range g.from.grants {
		if _, exists := g.findGrant(g.to.grants, fromGrant); !exists {
			if g.isDroppedGrant(fromGrant) {
				continue
			}
			ddl.AppendDDL(g.generateDDLForRevokeAll(fromGrant))
		}
	}
	for _, toGrant := range g.to.grants {
		if fromGrant, exists := g.findGrant(g.from.grants, toGrant); exists {
			if g.isDroppedGrant(fromGrant) {
				ddl.Append(toGrant)
			}
		} else {
			ddl.Append(toGrant)
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
			if csFor, ok := cs.For.(*ast.ChangeStreamForTables); ok && len(csFor.Tables) > 1 {
				var remainingTables []*ast.ChangeStreamForTable
				for _, t := range csFor.Tables {
					if identsToComparable(t.TableName) != identsToComparable(table.Name.Idents...) {
						remainingTables = append(remainingTables, t)
					}
				}
				hasRemainingTableInTarget := false
				for _, t := range remainingTables {
					if _, exists := g.findTableByName(g.to.tables, identsToComparable(t.TableName)); exists {
						hasRemainingTableInTarget = true
						break
					}
				}
				// change stream が g.to に存在する場合のみ ALTER 処理を行う
				// 存在しない場合（削除される場合）は DROP 処理に進む
				if len(remainingTables) > 0 && hasRemainingTableInTarget {
					if _, csExistsInTarget := g.findChangeStreamByName(g.to, identsToComparable(cs.Name)); csExistsInTarget {
						alteredCS := &ChangeStream{
							CreateChangeStream: &ast.CreateChangeStream{
								Name:    cs.Name,
								For:     &ast.ChangeStreamForTables{Tables: remainingTables},
								Options: cs.Options,
							},
						}
						g.alteredChangeStreamStates[identsToComparable(cs.Name)] = alteredCS

						// willCreateOrAlterChangeStreamIDs に登録されている場合は
						// ALTER文を出力しない（GenerateDDL で処理される）
						if _, willAlter := g.willCreateOrAlterChangeStreamIDs[identsToComparable(cs.Name)]; !willAlter {
							ddl.Append(&ast.AlterChangeStream{
								Name: cs.Name,
								ChangeStreamAlteration: &ast.ChangeStreamSetFor{
									For: &ast.ChangeStreamForTables{Tables: remainingTables},
								},
							})
						}
						continue
					}
				}
			}
			if _, exists := g.findChangeStreamByName(g.to, identsToComparable(cs.Name)); exists {
				if _, tableExists := g.findTableByName(g.to.tables, identsToComparable(table.Name.Idents...)); !tableExists {
					continue
				}
			}
			ddl.AppendDDL(g.generateDDLForDropChangeStream(cs))
			g.dropedChangeStream = append(g.dropedChangeStream, identsToComparable(cs.Name))
		}
	}
	ddl.AppendDDL(g.generateDDLForDropNamedConstraintsMatchingPredicate(func(_ *Table, constraint *ast.TableConstraint) bool {
		fk, ok := constraint.Constraint.(*ast.ForeignKey)
		if !ok {
			return false
		}
		return identsToComparable(fk.ReferenceTable.Idents...) == identsToComparable(table.Name.Idents...)
	}))
	grants := g.from.grantsOnTable(table)
	for _, grant := range grants {
		if g.isDroppedGrant(grant) {
			continue
		}
		g.droppedGrant = append(g.droppedGrant, grant)
	}
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

		if isColHidden(fromCol) != isColHidden(toCol) {
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

	ddl.AppendDDL(g.generateDDLForDropNamedConstraintsMatchingPredicate(func(t *Table, constraint *ast.TableConstraint) bool {
		fk, ok := constraint.Constraint.(*ast.ForeignKey)
		if !ok {
			return false
		}

		if identsToComparable(t.Name.Idents...) == identsToComparable(table.Idents...) {
			for _, c := range fk.Columns {
				if identsToComparable(column) == identsToComparable(c) {
					return true
				}
			}
		}

		if identsToComparable(fk.ReferenceTable.Idents...) == identsToComparable(table.Idents...) {
			for _, refColumn := range fk.ReferenceColumns {
				if identsToComparable(column) == identsToComparable(refColumn) {
					return true
				}
			}
		}

		return false
	}))

	grants := g.from.grantsFromPath(table)
	for _, grant := range grants {
		priv, ok := grant.Grant.Privilege.(*ast.PrivilegeOnTable)
		if !ok {
			continue
		}
		if !hasPrivilegeOnColumn(priv, column) {
			continue
		}
		if g.isDroppedGrant(grant) {
			continue
		}
		g.droppedGrant = append(g.droppedGrant, grant)
	}

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

		if exists && !g.indexEqualIgnoringStoring(fromIndex, toIndex) {
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

		if !exists || !g.indexEqualIgnoringStoring(fromIndex, toIndex) {
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
		if !exists || !g.indexEqualIgnoringStoring(fromIndex, toIndex) {
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
		if !exists || !g.indexEqualIgnoringStoring(fromIndex, toIndex) {
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

func (g *Generator) isDroppedGrant(grant *Grant) bool {
	for _, dg := range g.droppedGrant {
		if equalGrant(dg, grant) {
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
	return cmp.Equal(x, y,
		cmpopts.IgnoreTypes(token.Pos(0)),
		cmp.Comparer(func(a, b *ast.ForeignKey) bool {
			aVal := *a
			bVal := *b
			if aVal.OnDelete == "" {
				aVal.OnDelete = ast.OnDeleteNoAction
			}
			if bVal.OnDelete == "" {
				bVal.OnDelete = ast.OnDeleteNoAction
			}
			return cmp.Equal(aVal, bVal, cmpopts.IgnoreTypes(token.Pos(0)))
		}),
	)
}

func (g *Generator) indexEqualIgnoringStoring(x, y *ast.CreateIndex) bool {
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

func (g *Generator) generateDDLForDropNamedConstraintsMatchingPredicate(predicate func(table *Table, constraint *ast.TableConstraint) bool) DDL {
	ddl := DDL{}

	for _, table := range g.from.tables {
		for _, constraint := range table.TableConstraints {
			if predicate(table, constraint) {
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
		ddl.Append(&ast.AlterChangeStream{Name: to.Name, ChangeStreamAlteration: &ast.ChangeStreamDropForAll{}})
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
		if from.Options != nil {
			for _, r := range from.Options.Records {
				if optionsValueFromName(to.Options, r.Name.Name) == nil {
					options.Records = append(options.Records, &ast.OptionsDef{
						Name:  &ast.Ident{Name: r.Name.Name},
						Value: &ast.NullLiteral{},
					})
				}
			}
		}
		ddl.Append(&ast.AlterChangeStream{Name: to.Name, ChangeStreamAlteration: &ast.ChangeStreamSetOptions{Options: options}})
	}
	return ddl
}

func (g *Generator) generateDDLForDropChangeStream(changeStream *ChangeStream) DDL {
	ddl := DDL{}

	grants := g.from.grantsOnChangeStream(changeStream)
	for _, grant := range grants {
		if g.isDroppedGrant(grant) {
			continue
		}
		g.droppedGrant = append(g.droppedGrant, grant)
	}

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
	grants := g.from.grantsOnView(view)
	for _, grant := range grants {
		if g.isDroppedGrant(grant) {
			continue
		}
		g.droppedGrant = append(g.droppedGrant, grant)
	}

	ddl.Append(&ast.DropView{Name: view.Name})
	return ddl
}

func isColHidden(col *ast.ColumnDef) bool {
	return !col.Hidden.Invalid() && col.Hidden != token.Pos(0)
}

func (g *Generator) findRoleByName(roles []*Role, name string) (role *Role, exists bool) {
	for _, r := range roles {
		if identsToComparable(r.Name) == name {
			role = r
			exists = true
			break
		}
	}
	return
}

func (g *Generator) generateDDLForDropRole(role *Role) DDL {
	ddl := DDL{}
	grants := g.from.grantsOnRole(role)

	for _, grant := range grants {
		if g.isDroppedGrant(grant) {
			continue
		}
		g.droppedGrant = append(g.droppedGrant, grant)

		// If the resource doesn't exist in "to(Database)", skip REVOKE:
		// dropping the object means the grant no longer applies. Record droppedGrant only.
		if !g.existsGrantResourceIn(grant, g.to) {
			continue
		}
		ddl.AppendDDL(g.generateDDLForRevokeAll(grant))
	}
	ddl.Append(&ast.DropRole{
		Name: role.Name,
	})
	return ddl
}

func (g *Generator) findGrant(grants []*Grant, grant *Grant) (grantRole *Grant, exists bool) {
	for _, grt := range grants {
		if equalGrant(grt, grant) {
			grantRole = grt
			exists = true
			break
		}
	}
	return
}

func (g *Generator) generateDDLForRevokeAll(grant *Grant) DDL {
	ddl := DDL{}
	if len(grant.Roles) == 0 {
		return ddl
	}

	stmt := &ast.Revoke{
		Privilege: grant.Privilege,
		Roles:     grant.Roles,
	}

	ddl.Append(stmt)
	return ddl
}

// existsGrantResourceIn returns true if any target resource of the grant exists in the given database.
// Note: this checks resource existence (table/view/change stream…), not whether the grant itself exists.
func (g *Generator) existsGrantResourceIn(grant *Grant, database *Database) bool {
	if grant == nil || grant.Grant == nil || grant.Grant.Privilege == nil {
		return false
	}
	switch p := grant.Grant.Privilege.(type) {
	case *ast.PrivilegeOnTable:
		for _, name := range p.Names {
			if _, exists := g.findTableByName(database.tables, identsToComparable(name)); exists {
				return true
			}
		}
		return false

	case *ast.SelectPrivilegeOnView:
		for _, name := range p.Names {
			if _, exists := g.findViewByName(database.views, identsToComparable(name)); exists {
				return true
			}
		}
		return false

	case *ast.SelectPrivilegeOnChangeStream:
		for _, name := range p.Names {
			if _, exists := g.findChangeStreamByName(database, identsToComparable(name)); exists {
				return true
			}
		}
		return false

	case *ast.ExecutePrivilegeOnTableFunction:
		// Not tracked yet; return true so REVOKE precedes DROP ROLE (safe order).
		// TODO: Check existence when table functions are modeled.
		return true

	default:
		return false
	}
}

func equalGrant(a, b *Grant) bool {
	if a == nil || b == nil {
		return a == b
	}
	return equalAstGrant(a.Grant, b.Grant)
}

func equalAstGrant(a, b *ast.Grant) bool {
	if a == nil || b == nil {
		return a == b
	}
	if !equalPrivilege(a.Privilege, b.Privilege) {
		return false
	}
	return equalIdentLists(a.Roles, b.Roles)
}

func equalPrivilege(a, b ast.Privilege) bool {
	if a == nil || b == nil {
		return a == b
	}
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}

	switch aTyped := a.(type) {
	case *ast.PrivilegeOnTable:
		bTyped := b.(*ast.PrivilegeOnTable)
		if !equalIdentLists(aTyped.Names, bTyped.Names) {
			return false
		}
		return equalPrivilegesOnTable(aTyped.Privileges, bTyped.Privileges)
	case *ast.SelectPrivilegeOnChangeStream:
		bTyped := b.(*ast.SelectPrivilegeOnChangeStream)
		return equalIdentLists(aTyped.Names, bTyped.Names)
	case *ast.SelectPrivilegeOnView:
		bTyped := b.(*ast.SelectPrivilegeOnView)
		return equalIdentLists(aTyped.Names, bTyped.Names)
	case *ast.ExecutePrivilegeOnTableFunction:
		bTyped := b.(*ast.ExecutePrivilegeOnTableFunction)
		return equalIdentLists(aTyped.Names, bTyped.Names)
	case *ast.RolePrivilege:
		bTyped := b.(*ast.RolePrivilege)
		return equalIdentLists(aTyped.Names, bTyped.Names)
	default:
		return fmt.Sprintf("%#v", a) == fmt.Sprintf("%#v", b)
	}
}

func equalPrivilegesOnTable(a, b []ast.TablePrivilege) bool {
	if len(a) != len(b) {
		return false
	}
	for i, tp := range a {
		if !matchTablePrivilege(tp, b[i]) {
			return false
		}
	}
	return true
}

func matchTablePrivilege(a, b ast.TablePrivilege) bool {
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}
	switch aTyped := a.(type) {
	case *ast.SelectPrivilege:
		bTyped := b.(*ast.SelectPrivilege)
		return equalIdentLists(aTyped.Columns, bTyped.Columns)
	case *ast.InsertPrivilege:
		bTyped := b.(*ast.InsertPrivilege)
		return equalIdentLists(aTyped.Columns, bTyped.Columns)
	case *ast.UpdatePrivilege:
		bTyped := b.(*ast.UpdatePrivilege)
		return equalIdentLists(aTyped.Columns, bTyped.Columns)
	case *ast.DeletePrivilege:
		return true
	default:
		return fmt.Sprintf("%#v", a) == fmt.Sprintf("%#v", b)
	}
}

func equalIdentLists(a, b []*ast.Ident) bool {
	return identsToComparable(a...) == identsToComparable(b...)
}

func hasPrivilegeOnColumn(p *ast.PrivilegeOnTable, column *ast.Ident) bool {
	if p == nil || column == nil {
		return false
	}
	targetKey := identsToComparable(column)
	for _, privilege := range p.Privileges {
		switch t := privilege.(type) {
		case *ast.SelectPrivilege:
			for _, c := range t.Columns {
				if identsToComparable(c) == targetKey {
					return true
				}
			}
		case *ast.InsertPrivilege:
			for _, c := range t.Columns {
				if identsToComparable(c) == targetKey {
					return true
				}
			}
		case *ast.UpdatePrivilege:
			for _, c := range t.Columns {
				if identsToComparable(c) == targetKey {
					return true
				}
			}
		case *ast.DeletePrivilege:
			continue
		default:
			continue
		}
	}
	return false
}
