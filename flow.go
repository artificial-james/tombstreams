package tombstreams

// DoStream streams data from the outlet to inlet.
func DoStream(outlet Outlet, inlet Inlet) {
	outlet.Tomb().Go(func() error {
		defer close(inlet.In())
		for elem := range outlet.Out() {
			inlet.In() <- elem
		}

		return nil
	})
}
