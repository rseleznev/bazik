package balancer

import (
	"github.com/rseleznev/bazik/config"
)

type Balancer interface {
	Start()
}

type options struct {
	addr string // 127.0.0.1:5000
	proto string // tcp

	// Алгоритм балансировки
	balancingAlg string
	enablePipeline bool
	
	config.ServerOptions
}