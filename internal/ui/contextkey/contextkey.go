package contextkey

// Key identifies values shared between the admin middleware and layout.
// A private underlying type prevents collisions with context values from other packages.
type Key uint8

const (
	ActivePath Key = iota
	ActiveWorkspace
	WorkspacesList
)
