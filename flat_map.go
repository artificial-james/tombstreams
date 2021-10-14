package tombstreams

import (
	"sync"

	"gopkg.in/tomb.v2"
)

// FlatMapFunc is a FlatMap transformation function.
type FlatMapFunc func(interface{}) ([]interface{}, error)

// FlatMap takes one element and produces zero, one, or more elements.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//        |    |      |    |        |
//    [---------- FlatMapFunc ----------]
//        |    |           |   |    |
// out -- 1' - 2' -------- 4'- 4''- 5' -
type FlatMap struct {
	FlatMapF    FlatMapFunc
	in          chan interface{}
	out         chan interface{}
	parallelism uint
	t           *tomb.Tomb
}

// Verify FlatMap satisfies the Flow interface.
var _ Flow = (*FlatMap)(nil)

// NewFlatMap returns a new FlatMap instance.
// flatMapFunc is the FlatMap transformation function.
// parallelism is the flow parallelism factor. In case the events order matters, use parallelism = 1.
func NewFlatMap(t *tomb.Tomb, flatMapFunc FlatMapFunc, parallelism uint) *FlatMap {
	flatMap := &FlatMap{
		flatMapFunc,
		make(chan interface{}),
		make(chan interface{}),
		parallelism,
		t,
	}
	if t.Alive() {
		t.Go(flatMap.doStream)
	}
	return flatMap
}

// Via streams data through the given flow
func (fm *FlatMap) Via(flow Flow) Flow {
	if fm.t.Alive() {
		fm.t.Go(func() error {
			fm.transmit(flow)
			return nil
		})
	}
	return flow
}

// To streams data to the given sink
func (fm *FlatMap) To(sink Sink) {
	fm.transmit(sink)
}

// Out returns an output channel for sending data
func (fm *FlatMap) Out() <-chan interface{} {
	return fm.out
}

// In returns an input channel for receiving data
func (fm *FlatMap) In() chan<- interface{} {
	return fm.in
}

func (fm *FlatMap) Tomb() *tomb.Tomb {
	return fm.t
}

func (fm *FlatMap) transmit(inlet Inlet) {
	defer close(inlet.In())
	for {
		var e interface{}
		select {
		case elem, ok := <-fm.Out():
			if ok {
				e = elem
			} else {
				return
			}
		case <-fm.t.Dying():
			return
		}
		select {
		case inlet.In() <- e:
		case <-fm.t.Dying():
			return
		}
	}
}

func (fm *FlatMap) doStream() error {
	defer close(fm.out)

	var wg sync.WaitGroup
	wg.Add(int(fm.parallelism))
	for i := 0; i < int(fm.parallelism); i++ {
		if !fm.t.Alive() {
			break
		}
		fm.t.Go(func() error {
			defer wg.Done()
			for {
				var trans []interface{}
				var err error
				select {
				case elem, ok := <-fm.in:
					if ok {
						trans, err = fm.FlatMapF(elem)
						if err != nil {
							return err
						}
					} else {
						return nil
					}
				case <-fm.t.Dying():
					return nil
				}
				for _, item := range trans {
					select {
					case fm.out <- item:
					case <-fm.t.Dying():
						return nil
					}
				}
			}
			return nil
		})
	}

	wg.Wait()
	return nil
}
