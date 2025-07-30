package pgext

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"

	"github.com/huandu/go-sqlbuilder"
	"github.com/jackc/pgx/v5"
)

type SelectBuilder = func(sb *sqlbuilder.SelectBuilder)
type UpdateBuilder = func(ub *sqlbuilder.UpdateBuilder)

type Table[T any] struct {
	Name  string
	Txer  *Transactor
	Log   *slog.Logger
	RowTo pgx.RowToFunc[T]

	strct *sqlbuilder.Struct
}

func (t *Table[T]) FindAll(ctx context.Context, builder ...SelectBuilder) ([]T, error) {
	tx, err := t.Txer.Context(ctx)
	if err != nil {
		return nil, err
	}

	var sb *sqlbuilder.SelectBuilder

	strct := t.ensureStruct().WithoutTag("w")
	sb = strct.SelectFrom(t.Name)

	// t.log().Debug("read", "cols", strct.Columns())

	if len(builder) > 0 {
		builder[0](sb)
	}

	qs, args := sb.Build()
	t.log().Debug("select", "query", qs, "args", args)

	rows, err := tx.Underlying().Query(ctx, qs, args...)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback(ctx))
	}

	found, err := t.collectRows(rows)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback(ctx))
	}

	return found, tx.Commit(ctx)
}

func (t *Table[T]) Find(ctx context.Context, where ...SelectBuilder) (*T, error) {
	data, err := t.FindAll(ctx, func(sb *sqlbuilder.SelectBuilder) {
		for _, w := range where {
			w(sb)
		}

		sb.Limit(1)
	})

	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	return &data[0], nil
}

func (t *Table[T]) Update(
	ctx context.Context,
	record any,
	builder UpdateBuilder,
	returning ...bool,
) ([]T, error) {
	var ub *sqlbuilder.UpdateBuilder
	if record != nil {
		updStruct := t.ensureStruct().WithoutTag("r")
		ub = updStruct.Update(t.Name, record)
	} else {
		ub = sqlbuilder.PostgreSQL.NewUpdateBuilder().Update(t.Name)
	}

	if builder != nil {
		builder(ub)
	}

	// Don't do empty update query
	if ub.NumAssignment() == 0 {
		return []T{}, nil
	}

	qs, args := ub.Build()

	if len(returning) > 0 && returning[0] {
		qs = qs + " RETURNING " + strings.Join(t.ensureStruct().WithoutTag("w").Columns(), ", ")
	}

	t.log().Debug("update", "query", qs, "args", args)

	tx, err := t.Txer.Context(ctx)
	if err != nil {
		return nil, err
	}

	rows, err := tx.Underlying().Query(ctx, qs, args...)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback(ctx))
	}

	found, err := t.collectRows(rows)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback(ctx))
	}

	return found, tx.Commit(ctx)
}

func (t *Table[T]) Insert(ctx context.Context, data []*T, returning ...bool) ([]T, error) {
	if len(data) == 0 {
		return []T{}, nil
	}

	tx, err := t.Txer.Context(ctx)
	if err != nil {
		return nil, err
	}

	records := make([]any, len(data))
	for i, datum := range data {
		records[i] = datum
	}

	strct := t.ensureStruct().WithoutTag("r")
	sb := strct.InsertInto(t.Name, records...)

	if len(returning) > 0 && returning[0] {
		cols := strings.Join(t.ensureStruct().WithoutTag("w").Columns(), ",")
		sb = sb.Returning(cols)
	}

	qs, args := sb.Build()

	t.log().Debug("insert", "query", qs, "args", args)

	rows, err := tx.Underlying().Query(ctx, qs, args...)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback(ctx))
	}

	found, err := t.collectRows(rows)
	if err != nil {
		return nil, errors.Join(err, tx.Rollback(ctx))
	}

	return found, tx.Commit(ctx)
}

func (t *Table[T]) InsertOne(ctx context.Context, record *T, returning ...bool) (*T, error) {
	inserted, err := t.Insert(ctx, []*T{record}, returning...)
	if err != nil {
		return nil, err
	}

	if len(inserted) == 0 {
		return nil, nil
	}

	return &inserted[0], nil
}

func (t *Table[T]) collectRows(rows pgx.Rows) ([]T, error) {
	var found []T
	var err error

	if t.RowTo != nil {
		found, err = pgx.CollectRows(rows, t.RowTo)
	} else {
		found, err = pgx.CollectRows(rows, pgx.RowToStructByNameLax[T])
	}

	return found, err
}

func (t *Table[T]) ensureStruct() *sqlbuilder.Struct {
	if t.strct == nil {
		t.strct = sqlbuilder.NewStruct(new(T)).For(sqlbuilder.PostgreSQL)
	}

	return t.strct
}

func (t *Table[T]) log() *slog.Logger {
	if t.Log != nil {
		return t.Log.With("table", t.Name)
	}

	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
}
