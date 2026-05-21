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
	BalancerAlg string
	
	MainTimeout int
	RetryAmount int
	MaxClientsAmount int
	MaxIdleSeconds int
	DisableSocksPool bool
	MaxSocksPoolLen int
	InitialSocksPoolLen int
}

type ServerOptions struct {
	Addr Address

	RetryAmount int
	MaxClientsAmount int
	MaxIdleSeconds int
	DisableSocksPool bool
	MaxSocksPoolLen int
	InitialSocksPoolLen int
}

type Listener interface {
	Accept() (Conn, error)
}

type Conn interface {
	// Connect() error
	Close()
	GetFd() int
	CopyTo(Conn) error
	SetIdleDeadline(time.Time)
	LogActivity()
	LastActivity() time.Time
	SetLastActivity(time.Time)
	GetRawAddr() string
	CheckUnread() (int, error)
	CheckUnsent() (int, error)
}