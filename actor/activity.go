package quickstart

import (
	"context"
)

func Greet(ctx context.Context, name string) (string, error) {
	return "Hello " + name, nil
}
