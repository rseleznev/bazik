package balancer

import (
	"github.com/rseleznev/bazik/config"
	"github.com/rseleznev/bazik/internal/models"
)

type Balancer interface {
	Start()
}

type options struct {
	proto string
	
	ipVersion string // IPv4
	addr string // 127.0.0.1:5000

	// Алгоритм балансировки
	balancingAlg string
	
	// Количество попыток при неудаче прежде чем вернется ошибка
	retryAmount int

	// Количество секунд, за которое должен ответить получатель
	// (клиент или сервер)
	timeout int

	// Максимальное кол-во клиентов.
	// Когда лимит будет превышен, последующие соединения будут получать ошибку ECONNREFUSED
	maxClientsLimit int

	// Максимальное время бездействия соединения прежде чем оно будет закрыто
	maxChatIdleTime int

	// ...
}

type handler interface {
	Listen(chan *models.Client)
	TCPProxy(*models.Client, *models.Server) error
}

func NewBalancer(conf *config.Config, h handler) Balancer {
	var o options

	if conf.Proto == "tcp" {
		h, ok := h.(tcpHandler)
		if !ok {
			panic("balancer interface assert err")
		}
		
		return &TCPBalancer{
			opts: &o,
			newClients: make(chan *models.Client),

			handler: h,
		}	
	}

	return &TCPBalancer{}
}