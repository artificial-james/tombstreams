package tombstreams

import (
	"fmt"

	"gopkg.in/tomb.v2"
)

// ChanSink sends data to the output channel
type ChanSink struct {
	Out chan interface{}
}

// NewChanSink returns a new ChanSink instance
func NewChanSink(out chan interface{}) *ChanSink {
	return &ChanSink{out}
}

// In returns an input channel for receiving data
func (ch *ChanSink) In() chan<- interface{} {
	return ch.Out
}

// StdoutSink sends items to stdout
type StdoutSink struct {
	in chan interface{}
}

// NewStdoutSink returns a new StdoutSink instance
func NewStdoutSink(t *tomb.Tomb) *StdoutSink {
	sink := &StdoutSink{make(chan interface{})}
	sink.init(t)
	return sink
}

func (stdout *StdoutSink) init(t *tomb.Tomb) {
	if !t.Alive() {
		return
	}
	t.Go(func() error {
		for {
			select {
			case elem, ok := <-stdout.in:
				if ok {
					fmt.Println(elem)
				} else {
					return nil
				}
			case <-t.Dying():
				return nil
			}

		}
		return nil
	})
}

// In returns an input channel for receiving data
func (stdout *StdoutSink) In() chan<- interface{} {
	return stdout.in
}

// IgnoreSink sends items to /dev/null
type IgnoreSink struct {
	in chan interface{}
}

// NewIgnoreSink returns a new IgnoreSink instance
func NewIgnoreSink(t *tomb.Tomb) *IgnoreSink {
	sink := &IgnoreSink{make(chan interface{})}
	sink.init(t)
	return sink
}

func (ignore *IgnoreSink) init(t *tomb.Tomb) {
	if !t.Alive() {
		return
	}
	t.Go(func() error {
		for {
			select {
			case _, ok := <-ignore.in:
				if !ok {
					return nil
				}
			case <-t.Dying():
				return nil
			}
		}
		return nil
	})
}

// In returns an input channel for receiving data
func (ignore *IgnoreSink) In() chan<- interface{} {
	return ignore.in
}
