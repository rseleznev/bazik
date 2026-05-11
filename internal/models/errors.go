package models

import "errors"

var (
	// Polling
	ErrPollTimeout = errors.New("polling timeout")

	// Common
	ErrRetriesFailed = errors.New("all retries failed")
)