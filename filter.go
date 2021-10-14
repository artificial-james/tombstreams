package tombstreams

import (
	"sync"

	"gopkg.in/tomb.v2"
)

// FilterFunc is a filter predicate function.
type FilterFunc func(interface{}) (bool, error)

// Filter filters the incoming elements using a predicate.
// If the predicate returns true the element is passed downstream,
// if it returns false the element is discarded.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//        |    |      |    |        |
//    [---------- FilterFunc -----------]
//        |    |                    |
// out -- 1 -- 2 ------------------ 5 --
type Filter struct {
	FilterF     FilterFunc
	in          chan interface{}
	out         chan interface{}
	parallelism uint
	t           *tomb.Tomb
}

// Verify Filter satisfies the Flow interface.
var _ Flow = (*Filter)(nil)

// NewFilter returns a new Filter instance.
// filterFunc is the filter predicate function.
// parallelism is the flow parallelism factor. In case the events order matters, use parallelism = 1.
func NewFilter(t *tomb.Tomb, filterFunc FilterFunc, parallelism uint) *Filter {
	filter := &Filter{
		filterFunc,
		make(chan interface{}),
		make(chan interface{}),
		parallelism,
		t,
	}
	if t.Alive() {
		t.Go(filter.doStream)
	}
	return filter
}

// Via streams data through the given flow
func (f *Filter) Via(flow Flow) Flow {
	if f.t.Alive() {
		f.t.Go(func() error {
			f.transmit(flow)
			return nil
		})
	}
	return flow
}

// To streams data to the given sink
func (f *Filter) To(sink Sink) {
	f.transmit(sink)
}

// Out returns an output channel for sending data
func (f *Filter) Out() <-chan interface{} {
	return f.out
}

// In returns an input channel for receiving data
func (f *Filter) In() chan<- interface{} {
	return f.in
}

func (f *Filter) Tomb() *tomb.Tomb {
	return f.t
}

func (f *Filter) transmit(inlet Inlet) {
	defer close(inlet.In())
	for {
		var e interface{}
		select {
		case elem, ok := <-f.Out():
			if ok {
				e = elem
			} else {
				return
			}
		case <-f.t.Dying():
			return
		}
		select {
		case inlet.In() <- e:
		case <-f.t.Dying():
			return
		}
	}
}

// throws items that are not satisfying the filter function
func (f *Filter) doStream() error {
	defer close(f.out)

	var wg sync.WaitGroup
	wg.Add(int(f.parallelism))
	for i := 0; i < int(f.parallelism); i++ {
		if !f.t.Alive() {
			break
		}
		f.t.Go(func() error {
			defer wg.Done()
			for {
				var include bool
				var err error
				var e interface{}
				select {
				case elem, ok := <-f.in:
					if ok {
						include, err = f.FilterF(elem)
						if err != nil {
							return err
						}
						e = elem
					} else {
						return nil
					}
				case <-f.t.Dying():
					return nil
				}
				if include {
					select {
					case f.out <- e:
					case <-f.t.Dying():
						return nil
					}
				}
			}
		})
	}

	wg.Wait()
	return nil
}
