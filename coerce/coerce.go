package coerce

import (
	"errors"
	"fmt"
)

var ErrCoerce = errors.New("failed to coerce")

func AnySlice[T any](s []any) ([]T, error) {
	coerced := make([]T, len(s))

	for i, v := range s {
		tv, ok := v.(T)
		if !ok {
			return nil, errors.Join(
				ErrCoerce,
				fmt.Errorf("cannot coerce %d elem to type %T", i, tv),
			)
		}

		coerced[i] = tv
	}

	return coerced, nil
}
