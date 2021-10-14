package tombstreams

import (
	"sync"

	"gopkg.in/tomb.v2"
)

// MapFunc is a Map transformation function.
type MapFunc func(interface{}) (interface{}, error)

// Map takes one element and produces one element.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//        |    |      |    |        |
//    [----------- MapFunc -------------]
//        |    |      |    |        |
// out -- 1' - 2' --- 3' - 4' ----- 5' -
type Map struct {
	MapF        MapFunc
	in          chan interface{}
	out         chan interface{}
	parallelism uint
	t           *tomb.Tomb
}

// Verify Map satisfies the Flow interface.
var _ Flow = (*Map)(nil)

// NewMap returns a new Map instance.
// mapFunc is the Map transformation function.
// parallelism is the flow parallelism factor. In case the events order matters, use parallelism = 1.
func NewMap(t *tomb.Tomb, mapFunc MapFunc, parallelism uint) *Map {
	_map := &Map{
		mapFunc,
		make(chan interface{}),
		make(chan interface{}),
		parallelism,
		t,
	}
	if t.Alive() {
		t.Go(_map.doStream)
	}
	return _map
}

// Via streams data through the given flow
func (m *Map) Via(flow Flow) Flow {
	if m.t.Alive() {
		m.t.Go(func() error {
			m.transmit(flow)
			return nil
		})
	}
	return flow
}

// To streams data to the given sink
func (m *Map) To(sink Sink) {
	m.transmit(sink)
}

// Out returns an output channel for sending data
func (m *Map) Out() <-chan interface{} {
	return m.out
}

// In returns an input channel for receiving data
func (m *Map) In() chan<- interface{} {
	return m.in
}

func (m *Map) Tomb() *tomb.Tomb {
	return m.t
}

func (m *Map) transmit(inlet Inlet) {
	defer close(inlet.In())
	for {
		var e interface{}
		select {
		case elem, ok := <-m.Out():
			if ok {
				e = elem
			} else {
				return
			}
		case <-m.t.Dying():
			return
		}
		select {
		case inlet.In() <- e:
		case <-m.t.Dying():
			return
		}
	}
}

func (m *Map) doStream() error {
	defer close(m.out)

	var wg sync.WaitGroup
	wg.Add(int(m.parallelism))
	for i := 0; i < int(m.parallelism); i++ {
		if !m.t.Alive() {
			break
		}
		m.t.Go(func() error {
			defer wg.Done()
			for {
				var trans interface{}
				var err error
				select {
				case elem, ok := <-m.in:
					if ok {
						trans, err = m.MapF(elem)
						if err != nil {
							return err
						}
					} else {
						return nil
					}
				case <-m.t.Dying():
					return nil
				}
				select {
				case m.out <- trans:
				case <-m.t.Dying():
					return nil
				}
			}
			return nil
		})
	}

	wg.Wait()
	return nil
}
