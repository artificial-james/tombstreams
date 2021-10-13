package tombstreams

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
