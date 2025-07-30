package pgext

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

type PostgresError struct {
	*pgconn.PgError
}

func AsPostgresError(err error) *PostgresError {
	if err == nil {
		return nil
	}

	if pgErr, ok := err.(*PostgresError); ok {
		return pgErr
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return &PostgresError{
			PgError: pgErr,
		}
	}

	return nil
}

func (pe *PostgresError) IsUniqueViolation(col ...string) bool {
	return pe.Code == "23505" && (len(col) == 0 || col[0] == pe.ColumnName)
}
