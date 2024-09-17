package hammer

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/spanner/spansql"
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
		willCreateOrAlterChangeStreamIDs: map[spansql.ID]*ChangeStream{},
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
		alterDatabaseOptions *spansql.AlterDatabase
		options              spansql.SetDatabaseOptions
	)

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
		case *spansql.AlterDatabase:
			alterDatabaseOptions = stmt
			switch alteration := stmt.Alteration.(type) {
			case spansql.SetDatabaseOptions:
				options = alteration
			default:
				return nil, fmt.Errorf("unsupported database alteration: %v", stmt)
			}
		case *spansql.CreateChangeStream:
			if len(stmt.Watch) > 0 {
				for _, watch := range stmt.Watch {
					if t, ok := m[watch.Table]; ok {
						t.changeStreams = append(t.changeStreams, &ChangeStream{CreateChangeStream: stmt})
					}
				}
			} else {
				changeStreams = append(changeStreams, &ChangeStream{CreateChangeStream: stmt})
			}
		case *spansql.CreateView:
			v := &View{CreateView: stmt}
			views = append(views, v)
		case *spansql.CreateRole:
			roles = append(roles, &Role{CreateRole: stmt})
		case *spansql.GrantRole:
			grants = append(grants, &Grant{GrantRole: stmt})
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

	return &Database{tables: tables, changeStreams: changeStreams, views: views, roles: roles, grants: grants, alterDatabaseOptions: alterDatabaseOptions, options: options}, nil
}

type Database struct {
	tables        []*Table
	changeStreams []*ChangeStream
	views         []*View
	roles         []*Role
	grants        []*Grant

	alterDatabaseOptions *spansql.AlterDatabase
	options              spansql.SetDatabaseOptions
}

type Table struct {
	*spansql.CreateTable

	indexes       []*spansql.CreateIndex
	children      []*Table
	changeStreams []*ChangeStream
}

type View struct {
	*spansql.CreateView
}

type Role struct {
	*spansql.CreateRole
}

type Grant struct {
	*spansql.GrantRole
}

type ChangeStream struct {
	*spansql.CreateChangeStream
}

func (cs *ChangeStream) WatchNone() bool {
	return !cs.WatchAllTables && len(cs.Watch) == 0
}

func (cs *ChangeStream) WatchTable() bool {
	return !cs.WatchAllTables && len(cs.Watch) > 0
}

type Generator struct {
	from *Database
	to   *Database

	dropedTable                      []spansql.ID
	dropedIndex                      []spansql.ID
	dropedChangeStream               []spansql.ID
	droppedConstraints               []spansql.TableConstraint
	willCreateOrAlterChangeStreamIDs map[spansql.ID]*ChangeStream
}

func (g *Generator) GenerateDDL() DDL {
	ddl := DDL{}

	// for alter database
	ddl.AppendDDL(g.generateDDLForAlterDatabaseOptions())

	// for alter table
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
		ddl.AppendDDL(g.generateDDLForRowDeletionPolicy(fromTable, toTable))
		ddl.AppendDDL(g.generateDDLForCreateChangeStream(g.from, toTable))
	}
	for _, fromTable := range g.from.tables {
		if _, exists := g.findTableByName(g.to.tables, fromTable.Name); !exists {
			ddl.AppendDDL(g.generateDDLForDropConstraintIndexAndTable(fromTable))
		}
	}
	// for alter change stream
	for _, toChangeStream := range g.to.changeStreams {
		fromChangeStream, exists := g.findChangeStreamByName(g.from, toChangeStream.Name)
		if !exists {
			ddl.Append(toChangeStream)
			continue
		}
		ddl.AppendDDL(g.generateDDLForAlterChangeStream(fromChangeStream, toChangeStream))
	}
	for _, fromChangeStream := range g.from.changeStreams {
		if _, exists := g.findChangeStreamByName(g.to, fromChangeStream.Name); !exists {
			ddl.AppendDDL(g.generateDDLForDropChangeStream(fromChangeStream))
		}
	}
	for _, cs := range g.willCreateOrAlterChangeStreamIDs {
		fromChangeStream, exists := g.findChangeStreamByName(g.from, cs.Name)
		if !exists || g.isDropedChangeStream(cs.Name) {
			ddl.Append(cs)
			continue
		}

		ddl.AppendDDL(g.generateDDLForAlterChangeStream(fromChangeStream, cs))
	}
	// for views
	for _, toView := range g.to.views {
		_, exists := g.findViewByName(g.from.views, toView.Name)
		if !exists {
			ddl.Append(toView)
			continue
		}
		ddl.AppendDDL(g.generateDDLForReplaceView(toView))
	}
	for _, fromView := range g.from.views {
		if _, exists := g.findViewByName(g.to.views, fromView.Name); !exists {
			ddl.AppendDDL(g.generateDDLForDropView(fromView))
		}
	}
	// for roles
	for _, toRole := range g.to.roles {
		_, exists := g.findRoleByName(g.from.roles, toRole.Name)
		if !exists {
			ddl.Append(toRole)
			continue
		}
	}
	for _, fromRole := range g.from.roles {
		if _, exists := g.findRoleByName(g.to.roles, fromRole.Name); !exists {
			ddl.AppendDDL(g.generateDDLForDropRole(fromRole))
		}
	}
	// for grants
	for _, fromGrant := range g.from.grants {
		if _, exists := g.findGrant(g.to.grants, fromGrant.GrantRole); !exists {
			ddl.AppendDDL(g.generateDDLForRevokeRole(fromGrant))
		}
	}
	for _, toGrant := range g.to.grants {
		_, exists := g.findGrant(g.from.grants, toGrant.GrantRole)
		if !exists {
			ddl.Append(toGrant)
			continue
		}
	}

	return ddl
}

var (
	nullOptimizerVersion       = func(i int) *int { return &i }(0)
	nullVersionRetentionPeriod = func(s string) *string { return &s }("")
	nullEnableKeyVisualizer    = func(b bool) *bool { return &b }(false)
)

func (g *Generator) generateDDLForAlterDatabaseOptions() DDL {
	ddl := DDL{}
	if reflect.DeepEqual(g.to.options.Options, g.from.options.Options) {
		return ddl
	}
	if g.to.alterDatabaseOptions == nil {
		// set all null
		ddl.Append(&spansql.AlterDatabase{
			Name: g.from.alterDatabaseOptions.Name,
			Alteration: spansql.SetDatabaseOptions{
				Options: spansql.DatabaseOptions{
					VersionRetentionPeriod: nullVersionRetentionPeriod,
					OptimizerVersion:       nullOptimizerVersion,
					EnableKeyVisualizer:    nullEnableKeyVisualizer,
				},
			},
			Position: spansql.Position{},
		})
		return ddl
	}

	dbopts := spansql.DatabaseOptions{}

	if g.to.options.Options.OptimizerVersion != nil {
		dbopts.OptimizerVersion = g.to.options.Options.OptimizerVersion
	} else if g.from.options.Options.OptimizerVersion != nil {
		// from:specified, to:null
		dbopts.OptimizerVersion = nullOptimizerVersion
	}

	if g.to.options.Options.VersionRetentionPeriod != nil {
		dbopts.VersionRetentionPeriod = g.to.options.Options.VersionRetentionPeriod
	} else if g.from.options.Options.VersionRetentionPeriod != nil {
		// from:specified, to:null
		dbopts.VersionRetentionPeriod = nullVersionRetentionPeriod
	}

	if g.to.options.Options.EnableKeyVisualizer != nil {
		dbopts.EnableKeyVisualizer = g.to.options.Options.EnableKeyVisualizer
	} else if g.from.options.Options.EnableKeyVisualizer != nil {
		// from:specified, to:null
		dbopts.EnableKeyVisualizer = nullEnableKeyVisualizer
	}

	ddl.Append(&spansql.AlterDatabase{
		Name: g.to.alterDatabaseOptions.Name,
		Alteration: spansql.SetDatabaseOptions{
			Options: dbopts,
		},
		Position: spansql.Position{},
	})
	return ddl
}

func (g *Generator) generateDDLForCreateTableAndIndex(table *Table) DDL {
	ddl := DDL{}

	ddl.Append(table)
	for _, i := range table.indexes {
		ddl.Append(i)
	}
	for _, cs := range table.changeStreams {
		g.willCreateOrAlterChangeStreamIDs[cs.Name] = cs
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
	for _, cs := range table.changeStreams {
		if !g.isDropedChangeStream(cs.Name) {
			ddl.Append(spansql.DropChangeStream{Name: cs.Name})
			g.dropedChangeStream = append(g.dropedChangeStream, cs.Name)
		}
	}
	ddl.AppendDDL(g.generateDDLForDropNamedConstraintsMatchingPredicate(func(constraint spansql.TableConstraint) bool {
		fk, ok := constraint.Constraint.(spansql.ForeignKey)
		if !ok {
			return false
		}
		return fk.RefTable == table.Name
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

func (g *Generator) generateDDLForRowDeletionPolicy(from, to *Table) DDL {
	ddl := DDL{}

	switch {
	case from.RowDeletionPolicy != nil && to.RowDeletionPolicy != nil:
		if reflect.DeepEqual(from.RowDeletionPolicy, to.RowDeletionPolicy) {
			return ddl
		}
		ddl.Append(spansql.AlterTable{
			Name: to.Name,
			Alteration: spansql.ReplaceRowDeletionPolicy{
				RowDeletionPolicy: *to.RowDeletionPolicy,
			},
		})
	case from.RowDeletionPolicy != nil && to.RowDeletionPolicy == nil:
		ddl.Append(spansql.AlterTable{
			Name:       to.Name,
			Alteration: spansql.DropRowDeletionPolicy{},
		})
	case from.RowDeletionPolicy == nil && to.RowDeletionPolicy != nil:
		ddl.Append(spansql.AlterTable{
			Name: to.Name,
			Alteration: spansql.AddRowDeletionPolicy{
				RowDeletionPolicy: *to.RowDeletionPolicy,
			},
		})
	}

	return ddl
}
func (g *Generator) generateDDLForColumns(from, to *Table) DDL {
	ddl := DDL{}

	for _, toCol := range to.Columns {
		fromCol, exists := g.findColumnByName(from.Columns, toCol.Name)

		if !exists {
			if toCol.NotNull && toCol.Generated == nil && toCol.Default == nil {
				ddl.Append(spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: g.setDefault(toCol)}})
				ddl.Append(spansql.AlterTable{Name: to.Name, Alteration: spansql.AlterColumn{Name: toCol.Name, Alteration: spansql.DropDefault{}}})
			} else {
				ddl.Append(spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: toCol}})
			}
			continue
		}

		if g.columnDefEqual(fromCol, toCol) {
			continue
		}

		if g.columnTypeEqual(fromCol, toCol) && fromCol.Generated == nil && toCol.Generated == nil {
			if fromCol.Type.Base == spansql.Timestamp {
				if fromCol.NotNull != toCol.NotNull || !reflect.DeepEqual(fromCol.Default, toCol.Default) {
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

func (g *Generator) generateDDLForDropColumn(table spansql.ID, column spansql.ID) DDL {
	ddl := DDL{}

	ddl.AppendDDL(g.generateDDLForDropNamedConstraintsMatchingPredicate(func(constraint spansql.TableConstraint) bool {
		fk, ok := constraint.Constraint.(spansql.ForeignKey)
		if !ok {
			return false
		}
		for _, c := range fk.Columns {
			if column == c {
				return true
			}
		}

		for _, refColumn := range fk.RefColumns {
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

	if toCol.NotNull && toCol.Generated == nil && toCol.Default == nil {
		ddl.Append(spansql.AlterTable{Name: to.Name, Alteration: spansql.AddColumn{Def: g.setDefault(toCol)}})
		ddl.Append(spansql.AlterTable{Name: to.Name, Alteration: spansql.AlterColumn{Name: toCol.Name, Alteration: spansql.DropDefault{}}})
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

func (g *Generator) generateDDLForCreateChangeStream(from *Database, to *Table) DDL {
	ddl := DDL{}

	for _, cs := range to.changeStreams {
		g.willCreateOrAlterChangeStreamIDs[cs.Name] = cs
	}
	return ddl
}
func (g *Generator) generateDDLForDropNamedConstraint(table spansql.ID, constraint spansql.TableConstraint) DDL {
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

func (g *Generator) isDroppedConstraint(constraint spansql.TableConstraint) bool {
	for _, c := range g.droppedConstraints {
		if g.constraintEqual(c, constraint) {
			return true
		}
	}
	return false
}

func (g *Generator) isDropedChangeStream(name spansql.ID) bool {
	for _, t := range g.dropedChangeStream {
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
	return cmp.Equal(x, y,
		cmp.Comparer(func(x, y spansql.ID) bool {
			return strings.EqualFold(string(x), string(y))
		}),
		cmpopts.IgnoreTypes(spansql.Position{}),
		cmp.Comparer(func(x, y spansql.TimestampLiteral) bool {
			return time.Time(x).Equal(time.Time(y))
		}),
	)
}

func (g *Generator) columnTypeEqual(x, y spansql.ColumnDef) bool {
	return x.Type.Base == y.Type.Base && x.Type.Array == y.Type.Array
}

func (g *Generator) constraintEqual(x, y spansql.TableConstraint) bool {
	return cmp.Equal(x, y, cmpopts.IgnoreTypes(spansql.Position{}))
}

func (g *Generator) indexEqual(x, y spansql.CreateIndex) bool {
	return cmp.Equal(x, y, cmpopts.IgnoreTypes(spansql.Position{}))
}

func (g *Generator) watchEqual(x, y []spansql.WatchDef) bool {
	return cmp.Equal(x, y, cmpopts.IgnoreTypes(spansql.Position{}))
}

func (g *Generator) allowNull(col spansql.ColumnDef) spansql.ColumnDef {
	col.NotNull = false
	return col
}

func (g *Generator) setDefault(col spansql.ColumnDef) spansql.ColumnDef {
	if col.Type.Array {
		col.Default = spansql.Array{}
		return col
	}

	switch col.Type.Base {
	case spansql.Bool:
		col.Default = spansql.False
	case spansql.Int64:
		col.Default = spansql.IntegerLiteral(0)
	case spansql.Float64:
		col.Default = spansql.FloatLiteral(0)
	case spansql.Numeric:
		col.Default = spansql.FloatLiteral(0)
	case spansql.String:
		col.Default = spansql.StringLiteral("")
	case spansql.Bytes:
		col.Default = spansql.BytesLiteral("")
	case spansql.Date:
		col.Default = spansql.DateLiteral{Year: 1, Month: 1, Day: 1}
	case spansql.Timestamp:
		col.Default = spansql.TimestampLiteral(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))
	case spansql.JSON:
		col.Default = spansql.JSONLiteral("{}")
	}

	return col
}

func (g *Generator) findTableByName(tables []*Table, name spansql.ID) (table *Table, exists bool) {
	for _, t := range tables {
		if strings.EqualFold(string(t.Name), string(name)) {
			table = t
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findColumnByName(cols []spansql.ColumnDef, name spansql.ID) (col spansql.ColumnDef, exists bool) {
	for _, c := range cols {
		if strings.EqualFold(string(c.Name), string(name)) {
			col = c
			exists = true
			break
		}
	}
	return
}

func (g *Generator) findNamedConstraint(constraints []spansql.TableConstraint, name spansql.ID) (con spansql.TableConstraint, exists bool) {
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

func (g *Generator) findChangeStreamByName(database *Database, name spansql.ID) (changeStream *ChangeStream, exists bool) {
	for _, cs := range database.changeStreams {
		if cs.Name == name {
			changeStream = cs
			exists = true
			break
		}
	}
	for _, table := range database.tables {
		for _, cs := range table.changeStreams {
			if cs.Name == name {
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
	defaultRetentionPeriod := "1d"
	defaultValueCaptureType := "OLD_AND_NEW_VALUES"
	switch {
	case from.WatchAllTables && to.WatchTable():
		ddl.Append(spansql.AlterChangeStream{Name: to.Name, Alteration: spansql.AlterWatch{Watch: to.Watch}})
	case from.WatchAllTables && to.WatchNone():
		ddl.Append(spansql.AlterChangeStream{Name: to.Name, Alteration: spansql.DropChangeStreamWatch{}})
	case from.WatchTable() && to.WatchAllTables:
		ddl.Append(spansql.AlterChangeStream{Name: to.Name, Alteration: spansql.AlterWatch{WatchAllTables: to.WatchAllTables}})
	case from.WatchTable() && to.WatchNone():
		ddl.Append(spansql.DropChangeStream{Name: to.Name})
		ddl.Append(spansql.CreateChangeStream{Name: to.Name})
	case from.WatchTable() && to.WatchTable():
		if !g.watchEqual(from.Watch, to.Watch) {
			ddl.Append(spansql.AlterChangeStream{Name: to.Name, Alteration: spansql.AlterWatch{Watch: to.Watch}})
		}
	case from.WatchNone() && to.WatchAllTables:
		ddl.Append(spansql.AlterChangeStream{Name: to.Name, Alteration: spansql.AlterWatch{WatchAllTables: to.WatchAllTables}})
	case from.WatchNone() && to.WatchTable():
		ddl.Append(spansql.AlterChangeStream{Name: to.Name, Alteration: spansql.AlterWatch{Watch: to.Watch}})
	}
	if !reflect.DeepEqual(from.Options, to.Options) {
		if from.Options.RetentionPeriod != nil && to.Options.RetentionPeriod == nil {
			to.Options.RetentionPeriod = &defaultRetentionPeriod
		}
		if from.Options.ValueCaptureType != nil && to.Options.ValueCaptureType == nil {
			to.Options.ValueCaptureType = &defaultValueCaptureType
		}
		ddl.Append(spansql.AlterChangeStream{Name: to.Name, Alteration: spansql.AlterChangeStreamOptions{Options: to.Options}})
	}
	return ddl
}

func (g *Generator) generateDDLForDropChangeStream(changeStream *ChangeStream) DDL {
	ddl := DDL{}
	ddl.Append(spansql.DropChangeStream{Name: changeStream.Name})
	return ddl
}

func (g *Generator) findViewByName(views []*View, name spansql.ID) (view *View, exists bool) {
	for _, v := range views {
		if v.Name == name {
			view = v
			exists = true
			break
		}
	}
	return
}

func (g *Generator) generateDDLForReplaceView(view *View) DDL {
	ddl := DDL{}
	ddl.Append(spansql.CreateView{Name: view.Name, Position: view.Position, Query: view.Query, OrReplace: true})
	return ddl
}

func (g *Generator) generateDDLForDropView(view *View) DDL {
	ddl := DDL{}
	ddl.Append(spansql.DropView{Name: view.Name})
	return ddl
}

func (g *Generator) findRoleByName(roles []*Role, name spansql.ID) (role *Role, exists bool) {
	for _, r := range roles {
		if r.Name == name {
			role = r
			exists = true
			break
		}
	}
	return
}

func (g *Generator) generateDDLForDropRole(role *Role) DDL {
	ddl := DDL{}
	ddl.Append(spansql.DropRole{Name: role.Name})
	return ddl
}

func (g *Generator) findGrant(grants []*Grant, grant *spansql.GrantRole) (grantRole *Grant, exists bool) {
	for _, g := range grants {
		if reflect.DeepEqual(g.GrantRole, grant) {
			grantRole = g
			exists = true
			break
		}
	}
	return
}

func (g *Generator) generateDDLForRevokeRole(grant *Grant) DDL {
	ddl := DDL{}
	ddl.Append(spansql.RevokeRole{
		FromRoleNames:     grant.ToRoleNames,
		RevokeRoleNames:   grant.GrantRoleNames,
		Privileges:        grant.Privileges,
		TableNames:        grant.TableNames,
		TvfNames:          grant.TvfNames,
		ViewNames:         grant.ViewNames,
		ChangeStreamNames: grant.ChangeStreamNames,
	})
	return ddl
}
