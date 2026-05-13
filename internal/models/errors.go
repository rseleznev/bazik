package models

import "errors"

var (
	// Polling
	ErrPollTimeout = errors.New("polling timeout")

	// Common
	ErrTimeout = errors.New("timeout reached")
	ErrIdleTimeout = errors.New("idle timeout reached") // таймаут бездействия

	// Server
)