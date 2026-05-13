package balancer

import (
	"log"
	"math/rand"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type networker interface {
	NewTCPListener() (conn, error)
	NewConn(models.Address) (conn, error)
}

type conn interface {
	Accept() conn
	CopyTo(conn) error
	SetIdleDeadline(time.Time)
	LogActivity()
	LastActivity() time.Time
	SetLastActivity(time.Time)
}

type TCPBalancer struct {
	opts *options
	servers []*server
	chats map[string]*chat

	net networker
}

func (b *TCPBalancer) run() {
	listener, err := b.net.NewTCPListener()
	if err != nil {
		log.Fatal(err)
	}

	for {
		newClient := listener.Accept()
		b.link(newClient)
	}
}

func (b *TCPBalancer) link(c conn) {
	s := b.findServer()

	chat := &chat{
		id: "client+server addr hash?",
		client: c,
		server: s,
	}
	b.chats[chat.id] = chat
	chat.tcpProxy()
}

func (b *TCPBalancer) findServer() conn {
	switch b.opts.balancingAlg {
	case "random":
		n := rand.Intn(len(b.servers))
		return b.servers[n].connPool[0]

	default:
		return b.servers[0].connPool[0]

	}
}