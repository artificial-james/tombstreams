package tombstreams

import "context"

func GenerateIDs(ctx context.Context, ids []string) <-chan interface{} {
	out := make(chan interface{})

	go func() {
		defer close(out)
		for _, id := range ids {
			select {
			case out <- id:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}
