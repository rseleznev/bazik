package models

import "errors"

var (
	// Polling
	ErrPollTimeout = errors.New("polling timeout")

	// Common
	ErrResponseTimeout = errors.New("response timeout reached")

	// Server
)