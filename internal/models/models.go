package models

import "time"

type Address struct {
	Raw string

	IP [4]byte
	Port int
}

type Server struct {
	Addr Address
	Opts ServerOptions
	LastActivity time.Time
	LastHealthCheck time.Time
	ConnectionsLen int
}

type ServerOptions struct {}

type Client struct {
	Sock int
	
	Addr Address
	LastActivity time.Time
}