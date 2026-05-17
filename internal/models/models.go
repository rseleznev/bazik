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

type Conn interface {
	Connect() error
	Accept() Conn
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