package controlplane

import "time"

// nowFn is indented so tests can pin time if needed.
var nowFn = func() int64 { return time.Now().Unix() }
