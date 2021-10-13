package tombstreams

import (
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
	go _map.doStream()
	return _map
}

// Via streams data through the given flow
func (m *Map) Via(flow Flow) Flow {
	go m.transmit(flow)
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
	for elem := range m.Out() {
		inlet.In() <- elem
	}
}

func (m *Map) doStream() {
	defer close(m.out)
	sem := make(chan struct{}, m.parallelism)
	for {
		select {
		case elem := <-m.in:
			sem <- struct{}{}
			go func(e interface{}) {
				defer func() { <-sem }()
				trans, err := m.MapF(e)
				if err != nil {
					m.t.Kill(err)
				}
				select {
				case m.out <- trans:
				case <-m.t.Dying():
					return
				}
			}(elem)
		case <-m.t.Dying():
			return
		}
	}
	for i := 0; i < int(m.parallelism); i++ {
		sem <- struct{}{}
	}
}

// func (m *Map) doStream() {
// 	defer close(m.out)
// 	sem := make(chan struct{}, m.parallelism)
// 	for {
// 		select {
// 		case elem := <-m.in:
// 			sem <- struct{}{}
//
// 			wrapperFn := func(e interface{}) func() error {
// 				return func() error {
// 					defer func() { <-sem }()
// 					trans, err := m.MapF(e)
// 					if err != nil {
// 						return err
// 					}
//
// 					select {
// 					case m.out <- trans:
// 					case <-m.t.Dying():
// 						fmt.Println("Mapper routine is dying")
// 						return nil
// 					}
// 					return nil
// 				}
// 			}
//
// 			m.t.Go(wrapperFn(elem))
// 		case <-m.t.Dying():
// 			fmt.Println("Mapper is dying")
// 			return
// 		}
// 	}
// 	// for elem := range m.in {
// 	// 	sem <- struct{}{}
// 	//
// 	// 	wrapperFn := func(e interface{}) func() error {
// 	// 		return func() error {
// 	// 			defer func() { <-sem }()
// 	// 			trans, err := m.MapF(e)
// 	// 			if err != nil {
// 	// 				return err
// 	// 			}
// 	//
// 	// 			select {
// 	// 			case m.out <- trans:
// 	// 			case <-m.t.Dying():
// 	// 				return nil
// 	// 			}
// 	// 			return nil
// 	// 		}
// 	// 	}
// 	//
// 	// 	m.t.Go(wrapperFn(elem))
// 	// }
// 	for i := 0; i < int(m.parallelism); i++ {
// 		sem <- struct{}{}
// 	}
// }
