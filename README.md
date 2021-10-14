# tombstreams

A stream processing library for Go that combines [go-streams](https://github.com/reugn/go-streams) and [tombs](https://github.com/go-tomb/tomb).

Most of the flow capabilities from go-streams are supported.  Go channels are the only supported connector.  Tombs were added to provide a way to cancel a pipeline and surface any errors.
