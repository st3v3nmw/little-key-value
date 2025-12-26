package store

// NotFoundError is a custom error type for when a key is not found
// It implements the standard error interface
type NotFoundError struct{}

// Error returns a string representation of the error
func (e *NotFoundError) Error() string {
	return "key not found"
}
