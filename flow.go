package tombstreams

import (
	"sync"
)

// DoStream streams data from the outlet to inlet.
func DoStream(outlet Outlet, inlet Inlet) {
	t := outlet.Tomb()
	if !t.Alive() {
		return
	}
	t.Go(func() error {
		defer close(inlet.In())
		for {
			var elem interface{}
			select {
			case e, ok := <-outlet.Out():
				if ok {
					elem = e
					// inlet.In() <- elem
				} else {
					return nil
				}
			case <-t.Dying():
				return nil
			}
			select {
			case inlet.In() <- elem:
			case <-t.Dying():
				return nil
			}
		}

		return nil
	})
}

// FanOut creates a number of identical flows from the single outlet.
// This can be useful when writing to multiple sinks is required.
func FanOut(outlet Outlet, magnitude int) []Flow {
	t := outlet.Tomb()
	out := make([]Flow, magnitude)
	for i := 0; i < magnitude; i++ {
		out[i] = NewPassThrough(t)
	}

	if t.Alive() {
		t.Go(func() error {
			defer func() {
				for i := 0; i < magnitude; i++ {
					close(out[i].In())
				}
			}()
			for {
				var e interface{}
				select {
				case elem, ok := <-outlet.Out():
					if ok {
						e = elem
					} else {
						return nil
					}
				case <-t.Dying():
					return nil
				}
				for _, socket := range out {
					select {
					case socket.In() <- e:
					case <-t.Dying():
						return nil
					}
				}
			}

			return nil
		})
	}

	return out
}

// Merge merges multiple flows into a single flow.
func Merge(outlets ...Flow) Flow {
	if len(outlets) < 1 {
		panic("No flows to merge")
	}

	aTomb := outlets[0].Tomb()
	merged := NewPassThrough(aTomb)
	var wg sync.WaitGroup
	wg.Add(len(outlets))

	for _, out := range outlets {
		t := out.Tomb()
		if t.Alive() {
			t.Go(func(outlet Outlet) func() error {
				return func() error {
					defer wg.Done()
					t := outlet.Tomb()
					for {
						var e interface{}
						select {
						case elem, ok := <-outlet.Out():
							if ok {
								e = elem
							} else {
								return nil
							}
						case <-t.Dying():
							return nil
						}
						select {
						case merged.In() <- e:
						case <-t.Dying():
							return nil
						}
					}

					return nil
				}
			}(out))
		}
	}

	if aTomb.Alive() {
		// close merged.In() on the last outlet close.
		aTomb.Go(func(wg *sync.WaitGroup) func() error {
			return func() error {
				wg.Wait()
				close(merged.In())
				return nil
			}
		}(&wg))
	}

	return merged
}
