package pgext

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/yandzee/go-pgext/coerce"
	"github.com/yandzee/gotx"
)

type Transactor = gotx.Transactor[pgx.Tx]

func NewTransactor(pool *pgxpool.Pool) *Transactor {
	return &gotx.Transactor[pgx.Tx]{
		Beginner: &PgxBeginner{
			Pool: pool,
		},
	}
}

type PgxBeginner struct {
	Pool *pgxpool.Pool
}

func (pgb *PgxBeginner) Begin(ctx context.Context, rawopts ...any) (
	*gotx.Transaction[pgx.Tx], error,
) {
	opts, err := coerce.AnySlice[pgx.TxOptions](rawopts)
	if err != nil {
		return nil, err
	}

	txOpts := pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
	}

	if len(opts) > 0 {
		txOpts = opts[0]
	}

	tx, err := pgb.Pool.BeginTx(ctx, txOpts)
	if err != nil {
		return nil, err
	}

	return gotx.WrapOwnedTransaction(ctx, tx), nil
}
