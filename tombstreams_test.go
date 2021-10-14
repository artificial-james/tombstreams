package tombstreams_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/tomb.v2"

	"github.com/artificial-james/tombstreams"
)

func generateCounter(ctx context.Context, count int) <-chan interface{} {
	out := make(chan interface{})

	go func() {
		defer close(out)
		for i := 0; i < count; i++ {
			select {
			case out <- i:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

func TestMap(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		size := 3
		mapp := func(in interface{}) (interface{}, error) {
			return fmt.Sprintf("Test-%d", in), nil
		}

		tb, ctx := tomb.WithContext(context.TODO())
		source := tombstreams.NewChanSource(tb, generateCounter(ctx, size))
		mapper := tombstreams.NewMap(tb, mapp, 2)

		out := make(chan interface{})
		sink := tombstreams.NewChanSink(out)

		tb.Go(func() error {
			source.Via(mapper).To(sink)
			return nil
		})

		actual := make([]string, 0, size)
		for e := range sink.Out {
			if s, ok := e.(string); ok {
				actual = append(actual, s)
			}
		}
		<-tb.Dead()

		assert.Equal(t, nil, tb.Err())
		assert.Equal(t, context.Canceled, ctx.Err())
		assert.ElementsMatch(t, []string{"Test-0", "Test-1", "Test-2"}, actual)
	})
	t.Run("Error", func(t *testing.T) {
		size := 3
		mapp := func(in interface{}) (interface{}, error) {
			if in == 1 {
				return nil, fmt.Errorf("error!")
			}
			return fmt.Sprintf("Test-%d", in), nil
		}

		tb, ctx := tomb.WithContext(context.TODO())
		source := tombstreams.NewChanSource(tb, generateCounter(ctx, size))
		mapper := tombstreams.NewMap(tb, mapp, 2)

		out := make(chan interface{})
		sink := tombstreams.NewChanSink(out)

		tb.Go(func() error {
			source.Via(mapper).To(sink)
			return nil
		})

		actual := make([]string, 0, size)
		for e := range sink.Out {
			if s, ok := e.(string); ok {
				actual = append(actual, s)
			}
		}
		<-tb.Dead()

		assert.EqualError(t, tb.Err(), "error!")
		assert.Equal(t, context.Canceled, ctx.Err())
		assert.Less(t, len(actual), size)
	})
	t.Run("Context Canceled", func(t *testing.T) {
		size := 6
		mapp := func(in interface{}) (interface{}, error) {
			time.Sleep(1 * time.Millisecond)
			return fmt.Sprintf("Test-%d", in), nil
		}

		incomingCtx, _ := context.WithTimeout(context.TODO(), 1*time.Millisecond)
		tb, ctx := tomb.WithContext(incomingCtx)
		source := tombstreams.NewChanSource(tb, generateCounter(ctx, size))
		mapper := tombstreams.NewMap(tb, mapp, 2)

		out := make(chan interface{})
		sink := tombstreams.NewChanSink(out)

		tb.Go(func() error {
			source.Via(mapper).To(sink)
			return nil
		})

		actual := make([]string, 0, size)
		for e := range sink.Out {
			if s, ok := e.(string); ok {
				actual = append(actual, s)
			}
		}
		<-tb.Dead()

		assert.Equal(t, context.DeadlineExceeded, tb.Err())
		assert.Equal(t, context.DeadlineExceeded, ctx.Err())
		assert.Less(t, len(actual), size)
	})
}

func TestFlatMap(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		size := 3
		mapp := func(in interface{}) ([]interface{}, error) {
			return []interface{}{fmt.Sprintf("A-%d", in), fmt.Sprintf("B-%d", in)}, nil
		}

		tb, ctx := tomb.WithContext(context.TODO())
		source := tombstreams.NewChanSource(tb, generateCounter(ctx, size))
		mapper := tombstreams.NewFlatMap(tb, mapp, 2)

		out := make(chan interface{})
		sink := tombstreams.NewChanSink(out)

		tb.Go(func() error {
			source.Via(mapper).To(sink)
			return nil
		})

		actual := make([]string, 0, size*2)
		for e := range sink.Out {
			if s, ok := e.(string); ok {
				actual = append(actual, s)
			}
		}
		<-tb.Dead()

		assert.Equal(t, nil, tb.Err())
		assert.Equal(t, context.Canceled, ctx.Err())
		assert.ElementsMatch(t, []string{"A-0", "B-0", "A-1", "B-1", "A-2", "B-2"}, actual)
	})
	t.Run("Error", func(t *testing.T) {
		size := 3
		mapp := func(in interface{}) ([]interface{}, error) {
			if in == 1 {
				return nil, fmt.Errorf("error!")
			}
			return []interface{}{fmt.Sprintf("A-%d", in), fmt.Sprintf("B-%d", in)}, nil
		}

		tb, ctx := tomb.WithContext(context.TODO())
		source := tombstreams.NewChanSource(tb, generateCounter(ctx, size))
		mapper := tombstreams.NewFlatMap(tb, mapp, 2)

		out := make(chan interface{})
		sink := tombstreams.NewChanSink(out)

		tb.Go(func() error {
			source.Via(mapper).To(sink)
			return nil
		})

		actual := make([]string, 0, size)
		for e := range sink.Out {
			if s, ok := e.(string); ok {
				actual = append(actual, s)
			}
		}
		<-tb.Dead()

		assert.EqualError(t, tb.Err(), "error!")
		assert.Equal(t, context.Canceled, ctx.Err())
		assert.Less(t, len(actual), size*2)
	})
}

func TestFilter(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		size := 3
		filter := func(in interface{}) (bool, error) {
			if i, ok := in.(int); ok {
				return i%2 == 0, nil
			}
			return false, nil
		}

		tb, ctx := tomb.WithContext(context.TODO())
		source := tombstreams.NewChanSource(tb, generateCounter(ctx, size))
		mapper := tombstreams.NewFilter(tb, filter, 2)

		out := make(chan interface{})
		sink := tombstreams.NewChanSink(out)

		tb.Go(func() error {
			source.Via(mapper).To(sink)
			return nil
		})

		actual := make([]int, 0, size)
		for e := range sink.Out {
			if s, ok := e.(int); ok {
				actual = append(actual, s)
			}
		}
		<-tb.Dead()

		assert.Equal(t, nil, tb.Err())
		assert.Equal(t, context.Canceled, ctx.Err())
		assert.ElementsMatch(t, []int{0, 2}, actual)
	})
	t.Run("Error", func(t *testing.T) {
		// t.Skip()
		size := 3
		filter := func(in interface{}) (bool, error) {
			if i, ok := in.(int); ok {
				if i == 1 {
					return false, fmt.Errorf("error!")
				}
				return i%2 == 0, nil
			}
			return false, nil
		}

		tb, ctx := tomb.WithContext(context.TODO())
		source := tombstreams.NewChanSource(tb, generateCounter(ctx, size))
		mapper := tombstreams.NewFilter(tb, filter, 2)
		sink := tombstreams.NewStdoutSink(tb)

		source.Via(mapper).To(sink)

		<-tb.Dead()

		assert.EqualError(t, tb.Err(), "error!")
		assert.Equal(t, context.Canceled, ctx.Err())
	})
}

func TestPipeline(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		size := 3
		filter := func(in interface{}) (bool, error) {
			if i, ok := in.(int); ok {
				return i%2 == 0, nil
			}
			return false, nil
		}
		mapp := func(in interface{}) (interface{}, error) {
			return fmt.Sprintf("Test-%d", in), nil
		}
		flatten := func(in interface{}) ([]interface{}, error) {
			return []interface{}{in, in}, nil
		}

		tb, ctx := tomb.WithContext(context.TODO())
		source := tombstreams.NewChanSource(tb, generateCounter(ctx, size))
		filt := tombstreams.NewFilter(tb, filter, 2)
		mapper := tombstreams.NewMap(tb, mapp, 2)
		flat := tombstreams.NewFlatMap(tb, flatten, 2)

		out := make(chan interface{})
		sink := tombstreams.NewChanSink(out)

		tb.Go(func() error {
			source.Via(filt).Via(mapper).Via(flat).To(sink)
			return nil
		})

		actual := make([]string, 0, size)
		for e := range sink.Out {
			if s, ok := e.(string); ok {
				actual = append(actual, s)
			}
		}
		<-tb.Dead()

		assert.Equal(t, nil, tb.Err())
		assert.Equal(t, context.Canceled, ctx.Err())
		assert.ElementsMatch(t, []string{"Test-0", "Test-0", "Test-2", "Test-2"}, actual)
	})
	t.Run("With Branches", func(t *testing.T) {
		size := 3
		filter := func(in interface{}) (bool, error) {
			if i, ok := in.(int); ok {
				return i%2 == 0, nil
			}
			return false, nil
		}
		mapp := func(in interface{}) (interface{}, error) {
			return fmt.Sprintf("Test-%d", in), nil
		}
		flatten := func(in interface{}) ([]interface{}, error) {
			return []interface{}{fmt.Sprintf("%d", in), fmt.Sprintf("%d", in)}, nil
		}

		tb, ctx := tomb.WithContext(context.TODO())
		source := tombstreams.NewChanSource(tb, generateCounter(ctx, size))
		filt := tombstreams.NewFilter(tb, filter, 2)
		mapper := tombstreams.NewMap(tb, mapp, 2)
		flat := tombstreams.NewFlatMap(tb, flatten, 2)

		out := make(chan interface{})
		sink := tombstreams.NewChanSink(out)

		tb.Go(func() error {
			startFlow := source.Via(filt)

			fans := tombstreams.FanOut(startFlow, 2)

			mapFlow := fans[0].Via(mapper)
			flatFlow := fans[1].Via(flat)

			tombstreams.Merge(mapFlow, flatFlow).To(sink)
			return nil
		})

		actual := make([]string, 0, size)
		for e := range sink.Out {
			if s, ok := e.(string); ok {
				actual = append(actual, s)
			}
		}
		<-tb.Dead()

		assert.Equal(t, nil, tb.Err())
		assert.Equal(t, context.Canceled, ctx.Err())
		assert.ElementsMatch(t, []string{"Test-0", "Test-2", "0", "0", "2", "2"}, actual)
	})
	t.Run("With Branch Errors", func(t *testing.T) {
		t.Skip()
		size := 3
		filter := func(in interface{}) (bool, error) {
			if i, ok := in.(int); ok {
				return i%2 == 0, nil
			}
			return false, nil
		}
		mapp := func(in interface{}) (interface{}, error) {
			return fmt.Sprintf("Test-%d", in), nil
		}
		flatten := func(in interface{}) ([]interface{}, error) {
			if i, ok := in.(int); ok {
				if i == 2 {
					return nil, fmt.Errorf("error!")
				}
			}
			return []interface{}{fmt.Sprintf("%d", in), fmt.Sprintf("%d", in)}, nil
		}

		tb, ctx := tomb.WithContext(context.TODO())
		source := tombstreams.NewChanSource(tb, generateCounter(ctx, size))
		filt := tombstreams.NewFilter(tb, filter, 2)
		mapper := tombstreams.NewMap(tb, mapp, 2)
		flat := tombstreams.NewFlatMap(tb, flatten, 2)
		sink := tombstreams.NewIgnoreSink(tb)

		startFlow := source.Via(filt)
		fans := tombstreams.FanOut(startFlow, 2)
		mapFlow := fans[0].Via(mapper)
		flatFlow := fans[1].Via(flat)
		tombstreams.Merge(mapFlow, flatFlow).To(sink)

		<-tb.Dead()

		assert.EqualError(t, tb.Err(), "error!")
		assert.Equal(t, context.Canceled, ctx.Err())
	})
}
