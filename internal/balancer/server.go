package balancer

import (

	"github.com/rseleznev/bazik/internal/models"
)

type server struct {
	id string
	
	addr models.Address
	activeConnectionsAmount int
	connPool []conn

	retryAmount int
	maxResponseSeconds int
	maxClientsAmount int
	maxIdleSeconds int
	disableSocksPool bool
	maxSocksPoolLen int
	initialSocksPoolLen int
}