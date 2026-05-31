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
	ErrWrongProto = errors.New("wrong proto type")
	ErrAddrAssert = errors.New("address assertion error")
	ErrNumInvalid = errors.New("invalid number value")

	// Server
	ErrNoConnsAvailable = errors.New("no more conns to the server available")

	// Config
	ErrTooLongIpAddress = errors.New("too long IP address")
	ErrTooShortIpAddress = errors.New("too short IP address")
	ErrPortNumOutOfRange = errors.New("port number is out of range")
	ErrPortInvalid = errors.New("port number is invalid")
	ErrUnsupportedBalancingAlg = errors.New("unsupported balancing algorithm")
	ErrUnsupportedProxyMode = errors.New("unsupported proxy mode")
	ErrNoMaxClientsAmount = errors.New("no clients amount max specified")
	ErrNoMaxIdleSeconds = errors.New("no max idle seconds specified")
)