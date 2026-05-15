package models

import "errors"

var (
	// Polling
	ErrPollTimeout = errors.New("polling timeout")

	// Common
	ErrTimeout = errors.New("timeout reached")
	ErrIdleTimeout = errors.New("idle timeout reached") // таймаут бездействия
	ErrNoRetriesAvailable = errors.New("no more retries available")

	// Server
	ErrNoConnsAvailable = errors.New("no more conns to the server available")
)