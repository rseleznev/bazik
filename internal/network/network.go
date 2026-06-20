package network

import (
	"log/slog"
	"sync"
	"syscall"

	"github.com/rseleznev/bazik/internal/models"
)

type poller interface {
	Add(models.PollingUnit) error
	StopUnitPolling(models.PollingUnit)
}

type net struct {
	sys syscaller
	poller poller
}

func NewNet(p poller) *net {
	return &net{
		sys: &realSyscalls{
			mu: sync.Mutex{},
			pipeFDs: make([]int, 2),
		},
		poller: p,
	}
}

func (n *net) NewTCPListener(addr models.Address) (models.Listener, error) {
	sFd, err := n.newSocket("tcp")
	if err != nil {
		slog.Error("ошибка создания сокета", "module", "network", "addr", addr.Raw, "err", err)
		return nil, err
	}
	s := &socket{
		fd: sFd,
		addr: addr,

		sys: n.sys,
		poller: n.poller,
	}
	err = s.bind()
	if err != nil {
		slog.Error("ошибка вызова bind", "module", "network", "addr", addr.Raw, "err", err)
		return nil, err
	}
	err = s.listen()
	if err != nil {
		slog.Error("ошибка вызова listen", "module", "network", "addr", addr.Raw, "err", err)
		return nil, err
	}
	
	return s, nil
}

func (n *net) NewTCPConn(addr models.Address) (models.Conn, error) {
	sFd, err := n.newSocket("tcp")
	if err != nil {
		slog.Error("ошибка создания сокета", "module", "network", "addr", addr.Raw, "err", err)
		return nil, err
	}
	return &socket{
		fd: sFd,
		addr: addr,

		sys: n.sys,
		poller: n.poller,
	}, nil
}

func (n *net) newSocket(proto string) (int, error) {
	if proto == "tcp" {
		return n.sys.NewSocket(syscall.AF_INET, syscall.SOCK_STREAM | syscall.SOCK_NONBLOCK, syscall.IPPROTO_TCP)
	}

	return 0, models.ErrWrongProto
}