package hammer

import (
	"fmt"
	"reflect"
	"slices"
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
		from:                      database1,
		to:                        database2,
		alteredChangeStreamStates: map[string]*ChangeStream{},
	}
	return generator.GenerateDDL(), nil
}

func NewDatabase(ddl DDL) (*Database, error) {
	var (
		schemas              []*Schema
		tables               []*Table
		changeStreams        []*ChangeStream
		views                []*View
		queues               []*Queue
		sequences            []*Sequence
		roles                []*Role
		grants               []*Grant
		alterDatabaseOptions *ast.AlterDatabase
		options              *ast.Options
	)

	m := make(map[string]*Table)
	for _, istmt := range ddl.List {
		switch stmt := istmt.(type) {
		case *ast.CreateSchema:
			schemas = append(schemas, &Schema{CreateSchema: stmt})
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
			if t, ok := m[identsToComparable(stmt.TableName.Idents...)]; ok {
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
			views = append(views, &View{CreateView: stmt})
		case *ast.CreateQueue:
			queues = append(queues, &Queue{CreateQueue: stmt})
		case *ast.CreateSequence:
			sequences = append(sequences, &Sequence{CreateSequence: stmt})
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
	for _, q := range queues {
		if i := q.Cluster; i != nil {
			if p, ok := m[identsToComparable(i.TableName.Idents...)]; ok {
				p.queues = append(p.queues, q)
			} else {
				return nil, fmt.Errorf("parent ddl %s not found", i.TableName.SQL())
			}
		}
	}

	return &Database{
		schemas:              schemas,
		tables:               tables,
		changeStreams:        changeStreams,
		views:                views,
		queues:               queues,
		sequences:            sequences,
		roles:                roles,
		grants:               grants,
		alterDatabaseOptions: alterDatabaseOptions,
		options:              options,
	}, nil
}

type Database struct {
	schemas              []*Schema
	tables               []*Table
	changeStreams        []*ChangeStream
	views                []*View
	queues               []*Queue
	sequences            []*Sequence
	roles                []*Role
	grants               []*Grant
	alterDatabaseOptions *ast.AlterDatabase
	options              *ast.Options
}

func (d *Database) grantsOnTable(table *Table) []*Grant {
	return d.grantsFromPath(table.Name)
}

func (d *Database) grantsFromPath(path *ast.Path) []*Grant {
	target := identsToComparable(path.Idents...)
	return grantsOn(d, func(p *ast.PrivilegeOnTable) []*ast.Path { return p.Names }, func(name string) bool { return name == target })
}

func (d *Database) grantsOnView(view *View) []*Grant {
	target := identsToComparable(view.Name.Idents...)
	return grantsOn(d, func(p *ast.SelectPrivilegeOnView) []*ast.Path { return p.Names }, func(name string) bool { return name == target })
}

func (d *Database) grantsOnChangeStream(cs *ChangeStream) []*Grant {
	target := identsToComparable(cs.Name)
	return grantsOn(d, func(p *ast.SelectPrivilegeOnChangeStream) []*ast.Path { return p.Names }, func(name string) bool { return name == target })
}

func (d *Database) grantsOnQueue(queue *Queue) []*Grant {
	target := identsToComparable(queue.Name.Idents...)
	return grantsOn(d, func(p *ast.PrivilegeOnQueue) []*ast.Path { return p.Names }, func(name string) bool { return strings.EqualFold(name, target) })
}

func (d *Database) grantsOnSequence(sequence *Sequence) []*Grant {
	target := identsToComparable(sequence.Name.Idents...)
	return grantsOn(d, func(p *ast.PrivilegeOnSequence) []*ast.Path { return p.Names }, func(name string) bool { return strings.EqualFold(name, target) })
}

func grantsOn[P ast.Privilege](d *Database, names func(P) []*ast.Path, match func(name string) bool) []*Grant {
	var result []*Grant
	for _, grant := range d.grants {
		p, ok := grant.Grant.Privilege.(P)
		if !ok {
			continue
		}
		if slices.ContainsFunc(names(p), func(n *ast.Path) bool { return match(identsToComparable(n.Idents...)) }) {
			result = append(result, grant)
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
	queues        []*Queue
	changeStreams []*ChangeStream
}

type Schema struct {
	*ast.CreateSchema
}

type Queue struct {
	*ast.CreateQueue
}

type View struct {
	*ast.CreateView
}

type Sequence struct {
	*ast.CreateSequence
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

type droppedConstraint struct {
	table      string
	constraint *ast.TableConstraint
}

type Generator struct {
	from *Database
	to   *Database

	droppedTables                  []string
	droppedIndexes                 []string
	droppedChangeStreams           []string
	droppedQueues                  []string
	droppedConstraints             []droppedConstraint
	droppedGrants                  []*Grant
	willCreateOrAlterChangeStreams []*ChangeStream
	alteredChangeStreamStates      map[string]*ChangeStream
}

func (g *Generator) GenerateDDL() DDL {
	ddl := DDL{}

	// for alter database
	ddl.AppendDDL(g.generateDDLForAlterDatabaseOptions())

	// create schemas before any object that may be created inside them
	for _, toSchema := range g.to.schemas {
		if _, exists := g.findSchemaByName(g.from.schemas, identsToComparable(toSchema.Name)); !exists {
			ddl.Append(toSchema)
		}
	}

	// create or alter sequences before any column default that may use them
	for _, toSequence := range g.to.sequences {
		fromSequence, exists := g.findSequenceByName(g.from.sequences, identsToComparable(toSequence.Name.Idents...))
		if !exists {
			ddl.Append(toSequence)
			continue
		}
		ddl.AppendDDL(g.generateDDLForAlterSequence(fromSequence, toSequence))
	}

	// drop views before any table or column they may reference is dropped
	for i := len(g.from.views) - 1; i >= 0; i-- {
		fromView := g.from.views[i]
		if _, exists := g.findViewByName(g.to.views, identsToComparable(fromView.Name.Idents...)); !exists {
			ddl.AppendDDL(g.generateDDLForDropView(fromView))
		}
	}

	// for alter table
	var constraintTargets []*Table
	for _, toTable := range g.to.tables {
		fromTable, exists := g.findTableByName(g.from.tables, identsToComparable(toTable.Name.Idents...))

		if !exists {
			ddl.AppendDDL(g.generateDDLForCreateTableAndIndex(toTable))
			continue
		}

		if g.isDroppedTable(identsToComparable(toTable.Name.Idents...)) {
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
		constraintTargets = append(constraintTargets, toTable)
		ddl.AppendDDL(g.generateDDLForRowDeletionPolicy(fromTable, toTable))
		ddl.AppendDDL(g.generateDDLForCreateChangeStream(g.from, toTable))
	}
	for _, toTable := range constraintTargets {
		fromTable, _ := g.findTableByName(g.from.tables, identsToComparable(toTable.Name.Idents...))
		ddl.AppendDDL(g.generateDDLForConstraints(fromTable, toTable))
	}
	// for queues
	for _, toQueue := range g.to.queues {
		name := identsToComparable(toQueue.Name.Idents...)
		fromQueue, exists := g.findQueueByName(g.from.queues, name)
		if !exists || g.isDroppedQueue(name) {
			ddl.Append(toQueue)
			continue
		}
		// ALTER QUEUE cannot change columns, primary keys or the parent table.
		if !g.queueColumnsEqual(fromQueue, toQueue) || !queueInterleaveAlterable(fromQueue.Cluster, toQueue.Cluster) {
			ddl.AppendDDL(g.generateDDLForDropQueue(fromQueue))
			ddl.Append(toQueue)
			continue
		}
		ddl.AppendDDL(g.generateDDLForAlterQueue(fromQueue, toQueue))
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
	for _, cs := range g.willCreateOrAlterChangeStreams {
		fromChangeStream, exists := g.findChangeStreamByName(g.from, identsToComparable(cs.Name))
		if !exists || g.isDroppedChangeStream(identsToComparable(cs.Name)) {
			ddl.Append(cs)
			continue
		}
		if alteredState, hasAlteredState := g.alteredChangeStreamStates[identsToComparable(cs.Name)]; hasAlteredState {
			fromChangeStream = alteredState
		}
		ddl.AppendDDL(g.generateDDLForAlterChangeStream(fromChangeStream, cs))
	}
	// drop queues
	for _, fromQueue := range g.from.queues {
		name := identsToComparable(fromQueue.Name.Idents...)
		if g.isDroppedQueue(name) {
			continue
		}
		if _, exists := g.findQueueByName(g.to.queues, name); !exists {
			ddl.AppendDDL(g.generateDDLForDropQueue(fromQueue))
		}
	}
	// drop tables
	for _, fromTable := range g.from.tables {
		if _, exists := g.findTableByName(g.to.tables, identsToComparable(fromTable.Name.Idents...)); !exists {
			ddl.AppendDDL(g.generateDDLForDropConstraintIndexAndTable(fromTable))
		}
	}
	// drop change streams
	for _, fromChangeStream := range g.from.changeStreams {
		if g.isDroppedChangeStream(identsToComparable(fromChangeStream.Name)) {
			continue
		}
		if _, exists := g.findChangeStreamByName(g.to, identsToComparable(fromChangeStream.Name)); !exists {
			ddl.AppendDDL(g.generateDDLForDropChangeStream(fromChangeStream))
			g.droppedChangeStreams = append(g.droppedChangeStreams, identsToComparable(fromChangeStream.Name))
		}
	}
	for _, fromTable := range g.from.tables {
		if _, exists := g.findTableByName(g.to.tables, identsToComparable(fromTable.Name.Idents...)); !exists {
			continue
		}
		for _, cs := range fromTable.changeStreams {
			if g.isDroppedChangeStream(identsToComparable(cs.Name)) {
				continue
			}
			if _, exists := g.findChangeStreamByName(g.to, identsToComparable(cs.Name)); !exists {
				ddl.AppendDDL(g.generateDDLForDropChangeStream(cs))
				g.droppedChangeStreams = append(g.droppedChangeStreams, identsToComparable(cs.Name))
			}
		}
	}
	// drop sequences after every column default that may use them has been dropped
	for _, fromSequence := range g.from.sequences {
		if _, exists := g.findSequenceByName(g.to.sequences, identsToComparable(fromSequence.Name.Idents...)); !exists {
			ddl.AppendDDL(g.generateDDLForDropSequence(fromSequence))
		}
	}
	// for views
	for _, toView := range g.to.views {
		fromView, exists := g.findViewByName(g.from.views, identsToComparable(toView.Name.Idents...))
		if !exists {
			ddl.Append(toView)
			continue
		}
		if g.viewEqual(fromView, toView) {
			continue
		}
		ddl.AppendDDL(g.generateDDLForReplaceView(fromView, toView))
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

	// drop schemas after every object inside them has been dropped
	for _, fromSchema := range g.from.schemas {
		if _, exists := g.findSchemaByName(g.to.schemas, identsToComparable(fromSchema.Name)); !exists {
			ddl.Append(&ast.DropSchema{Name: fromSchema.Name})
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
		g.addWillCreateOrAlterChangeStream(cs)
	}
	return ddl
}

func (g *Generator) addWillCreateOrAlterChangeStream(cs *ChangeStream) {
	if g.isWillCreateOrAlterChangeStream(identsToComparable(cs.Name)) {
		return
	}
	g.willCreateOrAlterChangeStreams = append(g.willCreateOrAlterChangeStreams, cs)
}

func (g *Generator) isWillCreateOrAlterChangeStream(name string) bool {
	for _, cs := range g.willCreateOrAlterChangeStreams {
		if identsToComparable(cs.Name) == name {
			return true
		}
	}
	return false
}

func (g *Generator) generateDDLForDropConstraintIndexAndTable(table *Table) DDL {
	ddl := DDL{}

	if g.isDroppedTable(identsToComparable(table.Name.Idents...)) {
		return ddl
	}
	for _, t := range table.children {
		ddl.AppendDDL(g.generateDDLForDropConstraintIndexAndTable(t))
	}
	for _, q := range table.queues {
		if !g.isDroppedQueue(identsToComparable(q.Name.Idents...)) {
			ddl.AppendDDL(g.generateDDLForDropQueue(q))
		}
	}
	for _, i := range table.indexes {
		ddl.Append(&ast.DropIndex{Name: i.Name})
	}
	for _, i := range table.searchIndexes {
		ddl.Append(&ast.DropSearchIndex{Name: i.Name})
	}
	for _, cs := range table.changeStreams {
		if !g.isDroppedChangeStream(identsToComparable(cs.Name)) {
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
				// Only run ALTER when the change stream exists in g.to.
				// If it does not exist in g.to (i.e. it will be dropped), continue to DROP logic.
				if len(remainingTables) > 0 && hasRemainingTableInTarget {
					if toCS, csExistsInTarget := g.findChangeStreamByName(g.to, identsToComparable(cs.Name)); csExistsInTarget {
						alteredCS := &ChangeStream{
							CreateChangeStream: &ast.CreateChangeStream{
								Name:    cs.Name,
								For:     &ast.ChangeStreamForTables{Tables: remainingTables},
								Options: cs.Options,
							},
						}
						g.alteredChangeStreamStates[identsToComparable(cs.Name)] = alteredCS

						// Do not emit ALTER here when it is already handled in GenerateDDL
						// via willCreateOrAlterChangeStreams.
						// Also emit intermediate SET FOR only when the final target is FOR TABLES.
						if !g.isWillCreateOrAlterChangeStream(identsToComparable(cs.Name)) && toCS.Type() == ChangeStreamTypeTables {
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
			g.droppedChangeStreams = append(g.droppedChangeStreams, identsToComparable(cs.Name))
		}
	}
	ddl.AppendDDL(g.generateDDLForDropNamedConstraintsMatchingPredicate(func(_ *Table, constraint *ast.TableConstraint) bool {
		fk, ok := constraint.Constraint.(*ast.ForeignKey)
		if !ok {
			return false
		}
		return identsToComparable(fk.ReferenceTable.Idents...) == identsToComparable(table.Name.Idents...)
	}))
	g.markGrantsDropped(g.from.grantsOnTable(table))
	ddl.Append(&ast.DropTable{Name: table.Name})
	g.droppedTables = append(g.droppedTables, identsToComparable(table.Name.Idents...))
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

		if !exists || g.isDroppedConstraint(to.Name, toConstraint) {
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

	if alteration := g.rowDeletionPolicyAlteration(from.RowDeletionPolicy, to.RowDeletionPolicy); alteration != nil {
		ddl.Append(&ast.AlterTable{Name: to.Name, TableAlteration: alteration})
	}
	return ddl
}

// rowDeletionPolicyAlteration is an alteration that both ALTER TABLE and ALTER QUEUE accept.
type rowDeletionPolicyAlteration interface {
	ast.TableAlteration
	ast.QueueAlteration
}

func (g *Generator) rowDeletionPolicyAlteration(from, to *ast.CreateRowDeletionPolicy) rowDeletionPolicyAlteration {
	switch {
	case from != nil && to != nil:
		if g.createRowDeletionPolicyEqual(from, to) {
			return nil
		}
		return &ast.ReplaceRowDeletionPolicy{RowDeletionPolicy: to.RowDeletionPolicy}
	case from != nil && to == nil:
		return &ast.DropRowDeletionPolicy{}
	case from == nil && to != nil:
		return &ast.AddRowDeletionPolicy{RowDeletionPolicy: to.RowDeletionPolicy}
	}
	return nil
}

func (g *Generator) generateDDLForColumns(from, to *Table) DDL {
	ddl := DDL{}

	for _, toCol := range to.Columns {
		fromCol, exists := g.findColumnByName(from.Columns, identsToComparable(toCol.Name))

		if !exists {
			ddl.AppendDDL(g.generateDDLForAddColumn(to.Name, toCol))
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
			notNullChanged := fromCol.NotNull != toCol.NotNull
			if st, ok := fromCol.Type.(*ast.ScalarSchemaType); ok && st.Name == ast.TimestampTypeName {
				if notNullChanged || !g.columnDefaultExprEqual(fromCol.DefaultSemantics, toCol.DefaultSemantics) {
					if notNullChanged {
						ddl.AppendDDL(g.generateDDLForDropForeignKeysReferencingColumn(to.Name, toCol.Name))
					}
					if !fromCol.NotNull && toCol.NotNull {
						ddl.Append(Update{Table: to.Name.SQL(), Def: toCol})
					}
					ddl.Append(AlterColumn{Table: to.Name.SQL(), Def: toCol})
				}
				if !g.optionsValueEqual(fromCol.Options, toCol.Options, "allow_commit_timestamp") {
					ddl.Append(AlterColumn{Table: to.Name.SQL(), Def: toCol, SetOptions: true})
				}
			} else {
				if notNullChanged {
					ddl.AppendDDL(g.generateDDLForDropForeignKeysReferencingColumn(to.Name, toCol.Name))
				}
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

func (g *Generator) generateDDLForAddColumn(table *ast.Path, col *ast.ColumnDef) DDL {
	ddl := DDL{}

	if !col.NotNull || col.DefaultSemantics != nil {
		ddl.Append(&ast.AlterTable{Name: table, TableAlteration: &ast.AddColumn{Column: col}})
		return ddl
	}
	// A NOT NULL column needs a default to fill existing rows.
	ddl.Append(&ast.AlterTable{Name: table, TableAlteration: &ast.AddColumn{Column: withZeroDefault(col)}})
	ddl.Append(&ast.AlterTable{Name: table, TableAlteration: &ast.AlterColumn{Name: col.Name, Alteration: &ast.AlterColumnDropDefault{}}})
	return ddl
}

func (g *Generator) generateDDLForDropForeignKeysReferencingColumn(table *ast.Path, column *ast.Ident) DDL {
	return g.generateDDLForDropNamedConstraintsMatchingPredicate(func(t *Table, constraint *ast.TableConstraint) bool {
		return foreignKeyReferencesColumn(t, table, column, constraint)
	})
}

func (g *Generator) generateDDLForDropColumn(table *ast.Path, column *ast.Ident) DDL {
	ddl := DDL{}

	ddl.AppendDDL(g.generateDDLForDropNamedConstraintsMatchingPredicate(func(t *Table, constraint *ast.TableConstraint) bool {
		return foreignKeyReferencesColumn(t, table, column, constraint) || checkReferencesColumn(t, table, column, constraint)
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
		if !g.markGrantDropped(grant) {
			continue
		}
		// A grant that also covers other columns survives the drop on them, so it is revoked first.
		if !grantOnColumnOnly(priv, column) {
			ddl.AppendDDL(g.generateDDLForRevokeAll(grant))
		}
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
		if !g.isDroppedIndex(identsToComparable(i.Name.Idents...)) {
			indexes = append(indexes, i)
		}
	}
	for _, i := range indexes {
		ddl.Append(&ast.DropIndex{Name: i.Name})
	}

	searchIndexes := []*ast.CreateSearchIndex{}
	for _, i := range g.findSearchIndexByColumn(from.searchIndexes, identsToComparable(fromCol.Name)) {
		if !g.isDroppedIndex(identsToComparable(i.Name.Idents...)) {
			searchIndexes = append(searchIndexes, i)
		}
	}
	for _, i := range searchIndexes {
		ddl.Append(&ast.DropSearchIndex{Name: i.Name})
	}

	ddl.AppendDDL(g.generateDDLForDropColumn(from.Name, fromCol.Name))

	ddl.AppendDDL(g.generateDDLForAddColumn(to.Name, toCol))
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
			g.droppedIndexes = append(g.droppedIndexes, identsToComparable(fromIndex.Name.Idents...))
		}
	}
	for _, fromIndex := range from.indexes {
		if _, exists := g.findIndexByName(to.indexes, identsToComparable(fromIndex.Name.Idents...)); !exists {
			ddl.Append(&ast.DropIndex{Name: fromIndex.Name})
			g.droppedIndexes = append(g.droppedIndexes, identsToComparable(fromIndex.Name.Idents...))
		}
	}

	for _, toIndex := range to.searchIndexes {
		fromIndex, exists := g.findSearchIndexByName(from.searchIndexes, identsToComparable(toIndex.Name.Idents...))

		if exists && !g.searchIndexEqual(fromIndex, toIndex) {
			ddl.Append(&ast.DropSearchIndex{Name: fromIndex.Name})
			g.droppedIndexes = append(g.droppedIndexes, identsToComparable(fromIndex.Name.Idents...))
		}
	}
	for _, fromIndex := range from.searchIndexes {
		if _, exists := g.findSearchIndexByName(to.searchIndexes, identsToComparable(fromIndex.Name.Idents...)); !exists {
			ddl.Append(&ast.DropSearchIndex{Name: fromIndex.Name})
			g.droppedIndexes = append(g.droppedIndexes, identsToComparable(fromIndex.Name.Idents...))
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
		fromIndex, exists := g.findSearchIndexByName(from.searchIndexes, identsToComparable(toIndex.Name.Idents...))

		if !exists || !g.searchIndexEqual(fromIndex, toIndex) {
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
		g.addWillCreateOrAlterChangeStream(cs)
	}
	return ddl
}

func (g *Generator) generateDDLForDropNamedConstraint(table *ast.Path, constraint *ast.TableConstraint) DDL {
	ddl := DDL{}

	if constraint.Name == nil {
		return ddl
	}

	if g.isDroppedConstraint(table, constraint) {
		return ddl
	}
	g.droppedConstraints = append(g.droppedConstraints, droppedConstraint{table: identsToComparable(table.Idents...), constraint: constraint})

	ddl.Append(&ast.AlterTable{
		Name: table,
		TableAlteration: &ast.DropConstraint{
			Name: constraint.Name,
		},
	})

	return ddl
}

func (g *Generator) isDroppedTable(name string) bool {
	return slices.Contains(g.droppedTables, name)
}

func (g *Generator) isDroppedIndex(name string) bool {
	return slices.Contains(g.droppedIndexes, name)
}

func (g *Generator) isDroppedConstraint(table *ast.Path, constraint *ast.TableConstraint) bool {
	name := identsToComparable(table.Idents...)
	for _, c := range g.droppedConstraints {
		if strings.EqualFold(c.table, name) && g.constraintEqual(c.constraint, constraint) {
			return true
		}
	}
	return false
}

func (g *Generator) isDroppedChangeStream(name string) bool {
	return slices.Contains(g.droppedChangeStreams, name)
}

// markGrantDropped records grant as dropped and reports whether it was not recorded yet.
func (g *Generator) markGrantDropped(grant *Grant) bool {
	if g.isDroppedGrant(grant) {
		return false
	}
	g.droppedGrants = append(g.droppedGrants, grant)
	return true
}

func (g *Generator) markGrantsDropped(grants []*Grant) {
	for _, grant := range grants {
		g.markGrantDropped(grant)
	}
}

func (g *Generator) isDroppedGrant(grant *Grant) bool {
	for _, dg := range g.droppedGrants {
		if equalGrant(dg, grant) {
			return true
		}
	}
	return false
}

func (g *Generator) interleaveEqual(x, y *Table) bool {
	return cmp.Equal(x.Cluster, y.Cluster,
		ignorePos,
		cmp.Comparer(func(a, b *ast.Cluster) bool {
			if a == nil || b == nil {
				return a == b
			}
			aVal, bVal := *a, *b
			aVal.OnDelete = orDefault(a.OnDelete, ast.OnDeleteNoAction)
			bVal.OnDelete = orDefault(b.OnDelete, ast.OnDeleteNoAction)
			return cmp.Equal(aVal, bVal, ignorePos)
		}),
	)
}

func (g *Generator) primaryKeyEqual(x, y *Table) bool {
	xPrimaryKeys := primaryKeysForComparison(x)
	yPrimaryKeys := primaryKeysForComparison(y)
	if !g.indexKeysEqual(xPrimaryKeys, yPrimaryKeys) {
		return false
	}
	for _, pk := range yPrimaryKeys {
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

var (
	ignorePos = cmpopts.IgnoreTypes(token.Pos(0))

	indexKeyComparer = cmp.Comparer(func(a, b *ast.IndexKey) bool {
		aVal, bVal := *a, *b
		aVal.Dir = orDefault(a.Dir, ast.DirectionAsc)
		bVal.Dir = orDefault(b.Dir, ast.DirectionAsc)
		return cmp.Equal(aVal, bVal, ignorePos)
	})

	orderByItemComparer = cmp.Comparer(func(a, b *ast.OrderByItem) bool {
		aVal, bVal := *a, *b
		aVal.Dir = orDefault(a.Dir, ast.DirectionAsc)
		bVal.Dir = orDefault(b.Dir, ast.DirectionAsc)
		return cmp.Equal(aVal, bVal, ignorePos)
	})
)

func orDefault[T comparable](v, def T) T {
	var zero T
	if v == zero {
		return def
	}
	return v
}

func (g *Generator) indexKeysEqual(x, y []*ast.IndexKey) bool {
	return cmp.Equal(x, y, ignorePos, indexKeyComparer)
}

// primaryKeysForComparison folds the inline `id INT64 PRIMARY KEY` column-def
// form into the table-level `PRIMARY KEY (id)` form that Spanner's GetDdl
// always returns, so the two surface forms compare as equivalent.
func primaryKeysForComparison(table *Table) []*ast.IndexKey {
	if len(table.PrimaryKeys) > 0 {
		return table.PrimaryKeys
	}
	var keys []*ast.IndexKey
	for _, col := range table.Columns {
		if !col.PrimaryKey {
			continue
		}
		keys = append(keys, &ast.IndexKey{Name: col.Name})
	}
	return keys
}

func (g *Generator) columnDefEqual(x, y *ast.ColumnDef) bool {
	return cmp.Equal(x, y,
		ignorePos,
		cmpopts.IgnoreFields(ast.ColumnDef{}, "PrimaryKey"),
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
		ignorePos,
		cmp.Comparer(func(x, y *ast.SizedSchemaType) bool {
			return x.Name == y.Name
		}),
	)
}

func (g *Generator) constraintEqual(x, y *ast.TableConstraint) bool {
	return cmp.Equal(x, y,
		ignorePos,
		cmp.Comparer(func(a, b *ast.ForeignKey) bool {
			aVal, bVal := *a, *b
			aVal.OnDelete = orDefault(a.OnDelete, ast.OnDeleteNoAction)
			bVal.OnDelete = orDefault(b.OnDelete, ast.OnDeleteNoAction)
			aVal.Enforcement = orDefault(a.Enforcement, ast.Enforced)
			bVal.Enforcement = orDefault(b.Enforcement, ast.Enforced)
			return cmp.Equal(aVal, bVal, ignorePos)
		}),
	)
}

func (g *Generator) indexEqualIgnoringStoring(x, y *ast.CreateIndex) bool {
	return cmp.Equal(x, y,
		ignorePos,
		cmpopts.IgnoreTypes(&ast.Storing{}),
		cmpopts.IgnoreFields(ast.CreateIndex{}, "IfNotExists"),
		indexKeyComparer,
	)
}

func (g *Generator) searchIndexEqual(x, y *ast.CreateSearchIndex) bool {
	return cmp.Equal(x, y, ignorePos, orderByItemComparer)
}

func (g *Generator) changeStreamForEqual(x, y ast.ChangeStreamFor) bool {
	return cmp.Equal(x, y,
		ignorePos,
		cmpopts.SortSlices(func(a, b *ast.ChangeStreamForTable) bool {
			return identsToComparable(a.TableName) < identsToComparable(b.TableName)
		}),
		cmpopts.SortSlices(func(a, b *ast.Ident) bool {
			return identsToComparable(a) < identsToComparable(b)
		}),
	)
}

func (g *Generator) columnDefaultExprEqual(x, y ast.ColumnDefaultSemantics) bool {
	return cmp.Equal(x, y, ignorePos)
}

func (g *Generator) createRowDeletionPolicyEqual(x, y *ast.CreateRowDeletionPolicy) bool {
	return cmp.Equal(x, y, ignorePos)
}

func (g *Generator) optionsEqual(x, y *ast.Options) bool {
	return cmp.Equal(g.optionsToMap(x), g.optionsToMap(y))
}

func (g *Generator) optionsToMap(options *ast.Options) map[string]string {
	if options == nil {
		return nil
	}
	m := make(map[string]string, len(options.Records))
	for _, o := range options.Records {
		m[o.Name.Name] = o.Value.SQL()
	}
	return m
}

func (g *Generator) optionsValueEqual(x, y *ast.Options, name string) bool {
	xv := optionsValueFromName(x, name)
	yv := optionsValueFromName(y, name)
	return cmp.Equal(xv, yv, ignorePos)
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

func withZeroDefault(col *ast.ColumnDef) *ast.ColumnDef {
	c := *col
	switch t := col.Type.(type) {
	case *ast.ArraySchemaType:
		c.DefaultSemantics = &ast.ColumnDefaultExpr{Expr: &ast.ArrayLiteral{Values: nil}}
	case *ast.ScalarSchemaType:
		c.DefaultSemantics = &ast.ColumnDefaultExpr{Expr: defaultByScalarTypeName(t.Name)}
	case *ast.SizedSchemaType:
		c.DefaultSemantics = &ast.ColumnDefaultExpr{Expr: defaultByScalarTypeName(t.Name)}
	case *ast.NamedType:
		panic("not implemented")
	}
	return &c
}

func findFirst[T any](items []T, match func(T) bool) (T, bool) {
	for _, item := range items {
		if match(item) {
			return item, true
		}
	}
	var zero T
	return zero, false
}

func (g *Generator) findTableByName(tables []*Table, name string) (*Table, bool) {
	return findFirst(tables, func(t *Table) bool { return strings.EqualFold(identsToComparable(t.Name.Idents...), name) })
}

func (g *Generator) findColumnByName(cols []*ast.ColumnDef, name string) (*ast.ColumnDef, bool) {
	return findFirst(cols, func(c *ast.ColumnDef) bool { return strings.EqualFold(identsToComparable(c.Name), name) })
}

func (g *Generator) findIdentByName(idents []*ast.Ident, name string) (*ast.Ident, bool) {
	return findFirst(idents, func(i *ast.Ident) bool { return strings.EqualFold(identsToComparable(i), name) })
}

func (g *Generator) findNamedConstraint(constraints []*ast.TableConstraint, name string) (*ast.TableConstraint, bool) {
	if name == "" {
		return nil, false
	}
	return findFirst(constraints, func(c *ast.TableConstraint) bool { return identsToComparable(c.Name) == name })
}

func (g *Generator) findUnnamedConstraint(constraints []*ast.TableConstraint, item *ast.TableConstraint) (*ast.TableConstraint, bool) {
	return findFirst(constraints, func(c *ast.TableConstraint) bool { return g.constraintEqual(c, item) })
}

func (g *Generator) findIndexByName(indexes []*ast.CreateIndex, name string) (*ast.CreateIndex, bool) {
	return findFirst(indexes, func(i *ast.CreateIndex) bool { return identsToComparable(i.Name.Idents...) == name })
}

func (g *Generator) findIndexByColumn(indexes []*ast.CreateIndex, column string) []*ast.CreateIndex {
	result := []*ast.CreateIndex{}
	for _, i := range indexes {
		if indexReferencesColumn(i, column) {
			result = append(result, i)
		}
	}
	return result
}

func indexReferencesColumn(i *ast.CreateIndex, column string) bool {
	for _, k := range i.Keys {
		if identsToComparable(k.Name) == column {
			return true
		}
	}
	if i.Storing != nil {
		for _, c := range i.Storing.Columns {
			if identsToComparable(c) == column {
				return true
			}
		}
	}
	return false
}

func (g *Generator) findSearchIndexByName(indexes []*ast.CreateSearchIndex, name string) (*ast.CreateSearchIndex, bool) {
	return findFirst(indexes, func(i *ast.CreateSearchIndex) bool { return identsToComparable(i.Name.Idents...) == name })
}

func (g *Generator) findSearchIndexByColumn(indexes []*ast.CreateSearchIndex, column string) []*ast.CreateSearchIndex {
	result := []*ast.CreateSearchIndex{}
	for _, i := range indexes {
		if searchIndexReferencesColumn(i, column) {
			result = append(result, i)
		}
	}
	return result
}

func foreignKeyReferencesColumn(constraintHost *Table, tableName *ast.Path, column *ast.Ident, constraint *ast.TableConstraint) bool {
	fk, ok := constraint.Constraint.(*ast.ForeignKey)
	if !ok {
		return false
	}
	columnKey := identsToComparable(column)
	if identsToComparable(constraintHost.Name.Idents...) == identsToComparable(tableName.Idents...) {
		for _, c := range fk.Columns {
			if identsToComparable(c) == columnKey {
				return true
			}
		}
	}
	if identsToComparable(fk.ReferenceTable.Idents...) == identsToComparable(tableName.Idents...) {
		for _, refColumn := range fk.ReferenceColumns {
			if identsToComparable(refColumn) == columnKey {
				return true
			}
		}
	}
	return false
}

func checkReferencesColumn(constraintHost *Table, tableName *ast.Path, column *ast.Ident, constraint *ast.TableConstraint) bool {
	check, ok := constraint.Constraint.(*ast.Check)
	if !ok || identsToComparable(constraintHost.Name.Idents...) != identsToComparable(tableName.Idents...) {
		return false
	}
	// Any identifier with the column's name counts, so a function or field name may cause a needless re-add but never a missed one.
	for node := range ast.Preorder(check.Expr) {
		if id, ok := node.(*ast.Ident); ok && strings.EqualFold(id.Name, column.Name) {
			return true
		}
	}
	return false
}

func searchIndexReferencesColumn(i *ast.CreateSearchIndex, column string) bool {
	for _, c := range i.TokenListPart {
		if identsToComparable(c) == column {
			return true
		}
	}
	if i.Storing != nil {
		for _, c := range i.Storing.Columns {
			if identsToComparable(c) == column {
				return true
			}
		}
	}
	for _, c := range i.PartitionColumns {
		if identsToComparable(c) == column {
			return true
		}
	}
	if i.OrderBy != nil {
		for _, item := range i.OrderBy.Items {
			if id, ok := item.Expr.(*ast.Ident); ok && identsToComparable(id) == column {
				return true
			}
		}
	}
	return false
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

func (g *Generator) findChangeStreamByName(database *Database, name string) (*ChangeStream, bool) {
	match := func(cs *ChangeStream) bool { return identsToComparable(cs.Name) == name }
	if cs, ok := findFirst(database.changeStreams, match); ok {
		return cs, true
	}
	for _, table := range database.tables {
		if cs, ok := findFirst(table.changeStreams, match); ok {
			return cs, true
		}
	}
	return nil, false
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
		ddl.Append(&ast.AlterChangeStream{Name: to.Name, ChangeStreamAlteration: &ast.ChangeStreamSetOptions{Options: optionsForSet(from.Options, to.Options)}})
	}
	return ddl
}

// optionsForSet returns the options for SET OPTIONS that turn from into to.
// It resets the options that exist only in from to null.
func optionsForSet(from, to *ast.Options) *ast.Options {
	options := &ast.Options{}
	if to != nil {
		options.Records = append(options.Records, to.Records...)
	}
	if from != nil {
		for _, r := range from.Records {
			if optionsValueFromName(to, r.Name.Name) == nil {
				options.Records = append(options.Records, &ast.OptionsDef{
					Name:  &ast.Ident{Name: r.Name.Name},
					Value: &ast.NullLiteral{},
				})
			}
		}
	}
	return options
}

func (g *Generator) generateDDLForDropChangeStream(changeStream *ChangeStream) DDL {
	ddl := DDL{}

	g.markGrantsDropped(g.from.grantsOnChangeStream(changeStream))

	ddl.Append(&ast.DropChangeStream{Name: changeStream.Name})
	return ddl
}

func (g *Generator) findQueueByName(queues []*Queue, name string) (*Queue, bool) {
	return findFirst(queues, func(q *Queue) bool { return strings.EqualFold(identsToComparable(q.Name.Idents...), name) })
}

func (g *Generator) isDroppedQueue(name string) bool {
	return slices.ContainsFunc(g.droppedQueues, func(q string) bool { return strings.EqualFold(q, name) })
}

func (g *Generator) queueColumnsEqual(x, y *Queue) bool {
	if !g.indexKeysEqual(x.PrimaryKeys, y.PrimaryKeys) {
		return false
	}
	if len(x.Columns) != len(y.Columns) {
		return false
	}
	for _, yCol := range y.Columns {
		xCol, exists := g.findColumnByName(x.Columns, identsToComparable(yCol.Name))
		if !exists || isColHidden(xCol) != isColHidden(yCol) || !g.columnDefEqual(xCol, yCol) {
			return false
		}
	}
	return true
}

// queueInterleaveAlterable reports whether ALTER QUEUE can turn the interleave x into y.
// ALTER QUEUE can change PARENT and ON DELETE, but not the parent table itself.
func queueInterleaveAlterable(x, y *ast.Cluster) bool {
	if x == nil || y == nil {
		return x == y
	}
	return strings.EqualFold(identsToComparable(x.TableName.Idents...), identsToComparable(y.TableName.Idents...))
}

func queueOnDelete(c *ast.Cluster) ast.OnDeleteAction {
	if !c.Enforced {
		return ""
	}
	if c.OnDelete == "" {
		return ast.OnDeleteNoAction
	}
	return c.OnDelete
}

func (g *Generator) generateDDLForAlterQueue(from, to *Queue) DDL {
	ddl := DDL{}

	if x, y := from.Cluster, to.Cluster; x != nil && y != nil {
		switch {
		case x.Enforced == y.Enforced && queueOnDelete(x) == queueOnDelete(y):
			// The interleave has not changed.
		case x.Enforced && y.Enforced:
			ddl.Append(&ast.AlterQueue{Name: to.Name, QueueAlteration: &ast.SetOnDelete{OnDelete: queueOnDelete(y)}})
		case !x.Enforced && queueOnDelete(y) == ast.OnDeleteCascade:
			// Spanner cannot change INTERLEAVE IN to INTERLEAVE IN PARENT ON DELETE CASCADE in one step.
			ddl.Append(&ast.AlterQueue{Name: to.Name, QueueAlteration: &ast.SetInterleaveIn{TableName: y.TableName, Enforced: true, OnDelete: ast.OnDeleteNoAction}})
			ddl.Append(&ast.AlterQueue{Name: to.Name, QueueAlteration: &ast.SetOnDelete{OnDelete: ast.OnDeleteCascade}})
		default:
			ddl.Append(&ast.AlterQueue{Name: to.Name, QueueAlteration: &ast.SetInterleaveIn{TableName: y.TableName, Enforced: y.Enforced, OnDelete: y.OnDelete}})
		}
	}

	if alteration := g.rowDeletionPolicyAlteration(from.RowDeletionPolicy, to.RowDeletionPolicy); alteration != nil {
		ddl.Append(&ast.AlterQueue{Name: to.Name, QueueAlteration: alteration})
	}

	if !g.optionsEqual(from.Options, to.Options) {
		ddl.Append(&ast.AlterQueue{Name: to.Name, QueueAlteration: &ast.QueueSetOptions{Options: optionsForSet(from.Options, to.Options)}})
	}
	return ddl
}

func (g *Generator) generateDDLForDropQueue(queue *Queue) DDL {
	ddl := DDL{}

	// Dropping the queue also removes its grants, so record them as dropped without REVOKE.
	g.markGrantsDropped(g.from.grantsOnQueue(queue))

	ddl.Append(&ast.DropQueue{Name: queue.Name})
	g.droppedQueues = append(g.droppedQueues, identsToComparable(queue.Name.Idents...))
	return ddl
}

func (g *Generator) findSequenceByName(sequences []*Sequence, name string) (*Sequence, bool) {
	return findFirst(sequences, func(s *Sequence) bool { return strings.EqualFold(identsToComparable(s.Name.Idents...), name) })
}

func (g *Generator) generateDDLForDropSequence(sequence *Sequence) DDL {
	ddl := DDL{}
	g.markGrantsDropped(g.from.grantsOnSequence(sequence))
	ddl.Append(&ast.DropSequence{Name: sequence.Name})
	return ddl
}

func (g *Generator) generateDDLForAlterSequence(from, to *Sequence) DDL {
	ddl := DDL{}

	fromOptions := sequenceOptions(from.Options)
	toOptions := sequenceOptions(to.Options)

	var records []*ast.OptionsDef
	for _, name := range sequenceOptionNames(toOptions) {
		toValue := toOptions[name]
		if fromValue, ok := fromOptions[name]; ok && fromValue.SQL() == toValue.SQL() {
			continue
		}
		records = append(records, &ast.OptionsDef{Name: &ast.Ident{Name: name}, Value: toValue})
	}
	for _, name := range sequenceOptionNames(fromOptions) {
		if _, ok := toOptions[name]; ok {
			continue
		}
		// Spanner rejects a NULL sequence_kind, and a NULL start_with_counter
		// restarts the counter from 1, which can re-issue values already in use.
		if name == sequenceOptionKind || name == sequenceOptionStartWithCounter {
			continue
		}
		records = append(records, &ast.OptionsDef{Name: &ast.Ident{Name: name}, Value: &ast.NullLiteral{}})
	}
	if len(records) == 0 {
		return ddl
	}

	ddl.Append(&ast.AlterSequence{Name: to.Name, Options: &ast.Options{Records: records}})
	return ddl
}

func (g *Generator) findSchemaByName(schemas []*Schema, name string) (*Schema, bool) {
	return findFirst(schemas, func(s *Schema) bool { return identsToComparable(s.Name) == name })
}

func (g *Generator) findViewByName(views []*View, name string) (*View, bool) {
	return findFirst(views, func(v *View) bool { return identsToComparable(v.Name.Idents...) == name })
}

func (g *Generator) viewEqual(x, y *View) bool {
	return x.SecurityType == y.SecurityType && x.Query.SQL() == y.Query.SQL()
}

func (g *Generator) generateDDLForReplaceView(from, to *View) DDL {
	ddl := DDL{}
	// CREATE OR REPLACE VIEW removes the grants on the view.
	g.markGrantsDropped(g.from.grantsOnView(from))
	ddl.Append(&ast.CreateView{Name: to.Name, Query: to.Query, SecurityType: to.SecurityType, OrReplace: true})
	return ddl
}

func (g *Generator) generateDDLForDropView(view *View) DDL {
	ddl := DDL{}
	g.markGrantsDropped(g.from.grantsOnView(view))

	ddl.Append(&ast.DropView{Name: view.Name})
	return ddl
}

func isColHidden(col *ast.ColumnDef) bool {
	return !col.Hidden.Invalid() && col.Hidden != token.Pos(0)
}

func (g *Generator) findRoleByName(roles []*Role, name string) (*Role, bool) {
	return findFirst(roles, func(r *Role) bool { return identsToComparable(r.Name) == name })
}

func (g *Generator) generateDDLForDropRole(role *Role) DDL {
	ddl := DDL{}
	grants := g.from.grantsOnRole(role)

	for _, grant := range grants {
		if !g.markGrantDropped(grant) {
			continue
		}

		// If the resource doesn't exist in "to(Database)", skip REVOKE:
		// dropping the object means the grant no longer applies. Record droppedGrants only.
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

func (g *Generator) findGrant(grants []*Grant, grant *Grant) (*Grant, bool) {
	return findFirst(grants, func(grt *Grant) bool { return equalGrant(grt, grant) })
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
			if _, exists := g.findTableByName(database.tables, identsToComparable(name.Idents...)); exists {
				return true
			}
		}
		return false

	case *ast.SelectPrivilegeOnView:
		for _, name := range p.Names {
			if _, exists := g.findViewByName(database.views, identsToComparable(name.Idents...)); exists {
				return true
			}
		}
		return false

	case *ast.SelectPrivilegeOnChangeStream:
		for _, name := range p.Names {
			if _, exists := g.findChangeStreamByName(database, identsToComparable(name.Idents...)); exists {
				return true
			}
		}
		return false

	case *ast.PrivilegeOnQueue:
		for _, name := range p.Names {
			if _, exists := g.findQueueByName(database.queues, identsToComparable(name.Idents...)); exists {
				return true
			}
		}
		return false

	case *ast.PrivilegeOnSequence:
		for _, name := range p.Names {
			if _, exists := g.findSequenceByName(database.sequences, identsToComparable(name.Idents...)); exists {
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
		if !equalPathLists(aTyped.Names, bTyped.Names) {
			return false
		}
		return equalPrivilegesOnTable(aTyped.Privileges, bTyped.Privileges)
	case *ast.PrivilegeOnQueue:
		bTyped := b.(*ast.PrivilegeOnQueue)
		if !equalPathLists(aTyped.Names, bTyped.Names) {
			return false
		}
		return equalPrivilegesOnTable(aTyped.Privileges, bTyped.Privileges)
	case *ast.PrivilegeOnSequence:
		bTyped := b.(*ast.PrivilegeOnSequence)
		if !equalPathLists(aTyped.Names, bTyped.Names) {
			return false
		}
		return equalPrivilegesOnTable(aTyped.Privileges, bTyped.Privileges)
	case *ast.SelectPrivilegeOnChangeStream:
		bTyped := b.(*ast.SelectPrivilegeOnChangeStream)
		return equalPathLists(aTyped.Names, bTyped.Names)
	case *ast.SelectPrivilegeOnView:
		bTyped := b.(*ast.SelectPrivilegeOnView)
		return equalPathLists(aTyped.Names, bTyped.Names)
	case *ast.ExecutePrivilegeOnTableFunction:
		bTyped := b.(*ast.ExecutePrivilegeOnTableFunction)
		return equalPathLists(aTyped.Names, bTyped.Names)
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

func equalPathLists(a, b []*ast.Path) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if identsToComparable(a[i].Idents...) != identsToComparable(b[i].Idents...) {
			return false
		}
	}
	return true
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

func grantOnColumnOnly(p *ast.PrivilegeOnTable, column *ast.Ident) bool {
	if len(p.Names) != 1 {
		return false
	}
	target := identsToComparable(column)
	for _, privilege := range p.Privileges {
		var columns []*ast.Ident
		switch t := privilege.(type) {
		case *ast.SelectPrivilege:
			columns = t.Columns
		case *ast.InsertPrivilege:
			columns = t.Columns
		case *ast.UpdatePrivilege:
			columns = t.Columns
		default:
			return false
		}
		if len(columns) != 1 || identsToComparable(columns[0]) != target {
			return false
		}
	}
	return true
}
