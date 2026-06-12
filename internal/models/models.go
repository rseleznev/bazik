package models

import (
	"time"
)

type PollingUnit struct {
	SocketFd int
	EventType string // connect, income, outcome
	ResultChan chan error // канал, чтобы вызывающий поток заблокировался на чтении
}

type PollingResult struct {
	EventType int
	Err error
}

type Address struct {
	Raw string

	IP [4]byte
	Port int
}

type BalancerOptions struct {
	Addr Address
	Proto string
	BalancingAlg string
	
	MainTimeout int
	RetryAmount int
	MaxClientsAmount int
	MaxIdleSeconds int
	MaxConnsPoolLen int
	InitialConnsPoolLen int
}

type ServerOptions struct {
	Addr Address

	MainTimeout int
	MaxClientsAmount int
	MaxIdleSeconds int
	DisableConnsPool bool
	MaxConnsPoolLen int
	InitialConnsPoolLen int
}

type Listener interface {
	Accept() (Conn, error)
}

type Conn interface {
	Connect() error
	Close()
	GetFd() int
	CopyTo(Conn) error
	SetIdleTimeout(time.Duration)
	SetMainTimeout(time.Duration)
	LogActivity()
	LastActivity() <-chan time.Time
	GetRawAddr() string
	CheckUnread() (int, error)
	CheckUnsent() (int, error)
}