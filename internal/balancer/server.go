package balancer

import (
	"syscall"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type server struct {
	id string
	
	addr models.Address
	activeConnectionsAmount int

	retryAmount int
	maxResponseSeconds int
	maxClientsLimit int
	maxIdleSeconds int
	disableSocksPool bool
	maxSocksPoolLen int
	initialSocksPoolLen int
}

func (s *server) GetAddrIp4() syscall.SockaddrInet4 {
	return syscall.SockaddrInet4{
		Port: s.addr.Port,
		Addr: s.addr.IP,
	}
}

func (s *server) InitialPoolLen() int {
	return s.initialSocksPoolLen
}

func (s *server) MaxPoolLen() int {
	return s.maxSocksPoolLen
}

func (s *server) GetID() string {
	return s.id
}

func (s *server) GetTimeout() time.Duration {
	return time.Second*time.Duration(s.maxResponseSeconds)
}

func (s *server) GetRetries() int {
	return s.retryAmount
}