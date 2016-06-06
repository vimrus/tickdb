package store

import (
	"errors"
)

var (
	// ErrInvalid is returned when both meta pages on a database are invalid.
	// This typically occurs when a file is not a database.
	ErrInvalid = errors.New("invalid database")

	// ErrVersionMismatch is returned when the data file was created with a different version.
	ErrVersionMismatch = errors.New("version mismatch")

	// ErrChecksum is returned when either meta page checksum does not match.
	ErrChecksum = errors.New("checksum error")

	// ErrTimeout is returned when a database cannot obtain an exclusive lock
	// on the data file after the timeout passed to Open().
	ErrTimeout = errors.New("timeout")
)
