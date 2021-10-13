package tombstreams

// DoStream streams data from the outlet to inlet.
func DoStream(outlet Outlet, inlet Inlet) {
	go func() {
		for elem := range outlet.Out() {
			inlet.In() <- elem
		}

		close(inlet.In())
	}()
}
