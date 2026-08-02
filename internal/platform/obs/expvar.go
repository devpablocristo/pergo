package obs

import "expvar"

// AuditDrops counts audit events lost to queue overflow, persistence failure,
// or forced shutdown cancellation.
var AuditDrops = expvar.NewInt("audit_drops")
