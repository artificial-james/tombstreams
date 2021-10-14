package tombstreams

import "gopkg.in/tomb.v2"

// PassThrough produces the received element as is.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//        |    |      |    |        |
// out -- 1 -- 2 ---- 3 -- 4 ------ 5 --
type PassThrough struct {
	in  chan interface{}
	out chan interface{}
	t   *tomb.Tomb
}

// Verify PassThrough satisfies the Flow interface.
var _ Flow = (*PassThrough)(nil)

// NewPassThrough returns a new PassThrough instance.
func NewPassThrough(t *tomb.Tomb) *PassThrough {
	passThrough := &PassThrough{
		make(chan interface{}),
		make(chan interface{}),
		t,
	}
	if t.Alive() {
		t.Go(passThrough.doStream)
	}
	return passThrough
}

// Via streams data through the given flow
func (pt *PassThrough) Via(flow Flow) Flow {
	if pt.t.Alive() {
		pt.t.Go(func() error {
			pt.transmit(flow)
			return nil
		})
	}
	return flow
}

// To streams data to the given sink
func (pt *PassThrough) To(sink Sink) {
	pt.transmit(sink)
}

// Out returns an output channel for sending data
func (pt *PassThrough) Out() <-chan interface{} {
	return pt.out
}

// In returns an input channel for receiving data
func (pt *PassThrough) In() chan<- interface{} {
	return pt.in
}

func (pt *PassThrough) Tomb() *tomb.Tomb {
	return pt.t
}

func (pt *PassThrough) transmit(inlet Inlet) {
	defer close(inlet.In())
	for {
		var e interface{}
		select {
		case elem, ok := <-pt.Out():
			if ok {
				e = elem
			} else {
				return
			}
		case <-pt.t.Dying():
			return
		}
		select {
		case inlet.In() <- e:
		case <-pt.t.Dying():
			return
		}
	}
}

func (pt *PassThrough) doStream() error {
	defer close(pt.out)
	for {
		var e interface{}
		select {
		case elem, ok := <-pt.in:
			if ok {
				e = elem
			} else {
				return nil
			}
		case <-pt.t.Dying():
			return nil
		}
		select {
		case pt.out <- e:
		case <-pt.t.Dying():
			return nil
		}
	}

	return nil
}
