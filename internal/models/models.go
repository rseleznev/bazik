package models

type Address struct {
	Raw string

	IP [4]byte
	Port int
}

type Server interface {
	GetAddr() Address
}