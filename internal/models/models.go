package models

import (
	"syscall"
	"time"
)

type PollingUnit struct {
	SocketFd int
	EventType string // connect, income, outcome
	ResultChan chan error // канал, чтобы вызывающий поток заблокировался на чтении
}

type PollingResult struct {
	Err error
}

type Address struct {
	Raw string

	IP [4]byte
	Port int
}

type Client struct {
	Sock int
	
	Addr Address
	LastActivity time.Time
}

type Server interface {
	GetAddrIp4() syscall.SockaddrInet4
	InitialPoolLen() int
	MaxPoolLen() int
	GetID() string
	GetTimeout() time.Duration
	GetRetries() int
}