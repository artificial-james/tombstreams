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
				fmt.Println("Stopping...")
				return
			}
		}
	}()

	return out
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
	t, pctx := tomb.WithContext(ctx)
	// source := tombstreams.NewChanSource(t, generateTicker(ctx))
	source := tombstreams.NewChanSource(t, generateCounter(pctx, 20))
	mapper := tombstreams.NewMap(t, Map, 1)

	out := make(chan interface{})
	sink := tombstreams.NewChanSink(out)

	go func() {
		source.Via(mapper).To(sink)
	}()

	for e := range sink.Out {
		fmt.Println(e)
	}
	<-t.Dead()
	fmt.Println("Done")

	err := source.Tomb().Err()
	if err != nil {
		fmt.Printf("(Tomb) Exited with error:  %v\n", err)
	}
	err = ctx.Err()
	if err != nil {
		fmt.Printf("(Ctx) Exited with error:  %v\n", err)
	}
}
