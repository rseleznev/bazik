package models

import "errors"

var (
	// Polling
	ErrPollTimeout = errors.New("polling timeout")
	ErrPollUnknownEventType = errors.New("unknown event type for polling")
	ErrSocketEvent = errors.New("error event has happened on socket") // EPOLLERR event
	ErrSocketHUPEvent = errors.New("HUP error event has happened on socket") // EPOLLHUP event
	ErrSocketRDHUPEvent = errors.New("RDHUP error event has happened on socket") // EPOLLRDHUP event

	// Common
	ErrTimeout = errors.New("timeout reached")
	ErrIdleTimeout = errors.New("idle timeout reached") // таймаут бездействия
	ErrNoRetriesAvailable = errors.New("no more retries available")
	ErrClientSide = errors.New("error on client side")

	// Server
	ErrNoConnsAvailable = errors.New("no more conns to the server available")
)