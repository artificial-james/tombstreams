package main

import (
	"context"
	"fmt"
	"time"

	"gopkg.in/tomb.v2"

	"github.com/artificial-james/tombstreams"
)

func Map(in interface{}) (interface{}, error) {
	time.Sleep(500 * time.Millisecond)
	// Any errors cause the pipeline to cancel
	return in, nil
}

func generateCounter(ctx context.Context, count int) <-chan interface{} {
	out := make(chan interface{})

	go func() {
		defer close(out)
		for i := 0; i < count; i++ {
			select {
			case out <- i:
			case <-ctx.Done():
				// Supports tombs and a child context
				fmt.Println("Stopping...")
				return
			}
		}
	}()

	return out
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), 20*time.Second)
	t, pctx := tomb.WithContext(ctx)
	source := tombstreams.NewChanSource(t, generateCounter(pctx, 20))
	mapper := tombstreams.NewMap(t, Map, 2)
	sink := tombstreams.NewStdoutSink(t)

	// If using NewChanSink, the pipeline will need to be wrapped in tomb.Go
	source.Via(mapper).To(sink)

	<-t.Dead()
	fmt.Println("Done")

	err := source.Tomb().Err()
	if err != nil {
		fmt.Printf("(Tomb) Exited with error:  %v\n", err)
	}
	// The parent context is not affected by the state of the pipeline.
	// However, tomb will cancel the child context when the pipeline exits.
	err = ctx.Err()
	if err != nil {
		fmt.Printf("(Ctx) Exited with error:  %v\n", err)
	}
}
