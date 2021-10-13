package main

import (
	"context"
	"fmt"
	"time"

	"github.com/artificial-james/tombstreams"
	"gopkg.in/tomb.v2"
)

var counter = 0

func Map(in interface{}) (interface{}, error) {
	if counter == 12 {
		return nil, fmt.Errorf("Cannot map thing")
	}
	counter++
	return in, nil
}

func generateTicks(ctx context.Context) chan interface{} {
	ticker := time.NewTicker(1 * time.Second)
	out := make(chan interface{})

	go func() {
		for {
			select {
			case tick := <-ticker.C:
				out <- tick
			case <-ctx.Done():
				fmt.Println("Stopping...")
				ticker.Stop()
				return
			}
		}
	}()

	return out
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
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	t, ctx := tomb.WithContext(ctx)
	// source := tombstreams.NewChanSource(t, generateTicker(ctx))
	source := tombstreams.NewChanSource(t, generateCounter(ctx, 100))
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
		fmt.Printf("Exited with error:  %v\n", err)
	}
	err = ctx.Err()
	if err != nil {
		fmt.Printf("Exited with error:  %v\n", err)
	}
}
