package tombstreams

import (
	"gopkg.in/tomb.v2"
)

// ChanSource streams data from the input channel
type ChanSource struct {
	in <-chan interface{}
	t  *tomb.Tomb
}

// NewChanSource returns a new ChanSource instance
func NewChanSource(t *tomb.Tomb, in <-chan interface{}) *ChanSource {
	return &ChanSource{in, t}
}

// Via streams data through the given flow
func (cs *ChanSource) Via(_flow Flow) Flow {
	DoStream(cs, _flow)
	return _flow
}

// Out returns an output channel for sending data
func (cs *ChanSource) Out() <-chan interface{} {
	return cs.in
}

// Tomb returns the tomb context
func (cs *ChanSource) Tomb() *tomb.Tomb {
	return cs.t
}
