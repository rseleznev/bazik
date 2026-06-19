package network

import (
	"errors"
	"log/slog"
	"strconv"
	"syscall"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type socket struct {
	fd int
	addr models.Address
	timeout time.Duration
	
	cancelChan chan struct{}

	pipeWriteFd int
	pipeReadFd int
	dataInPipe bool
	
	sys syscaller
	poller poller
}

func (s *socket) bind() error {
	return s.sys.Bind(s.GetFd(), &syscall.SockaddrInet4{
		Addr: s.addr.IP,
		Port: s.addr.Port,
	})
}

func (s *socket) listen() error {
	return s.sys.Listen(s.GetFd(), 10)
}

func (s *socket) Accept() (models.Conn, error) {
	var sFd int
	var a syscall.Sockaddr
	var err error
	for {
		sFd, a, err = s.sys.Accept(s.GetFd())
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				err := s.poll("income")
				if err != nil {
					slog.Error("ошибка слушающего сокета", "module", "socket", "addr", s.addr.Raw, "err", err)
					return nil, err
				}

				continue		
			}
			slog.Error("ошибка слушающего сокета", "module", "socket", "addr", s.addr.Raw, "err", err)
			return nil, err
		}
		break
	}
	
	addr, ok := a.(*syscall.SockaddrInet4)
	if !ok {
		return nil, models.ErrAddrAssert
	}
	rawAddr := strconv.Itoa(int(addr.Addr[0])) + "." + strconv.Itoa(int(addr.Addr[1])) + "." + strconv.Itoa(int(addr.Addr[2])) + "." + strconv.Itoa(int(addr.Addr[3])) + ":" + strconv.Itoa(addr.Port)

	return &socket{
		fd: sFd,
		addr: models.Address{
			IP: addr.Addr,
			Port: addr.Port,
			Raw: rawAddr,
		},

		sys: s.sys,
		poller: s.poller,
	}, nil
}

func (s *socket) Connect() error {
	for {
		err := s.sys.Connect(s.GetFd(), &syscall.SockaddrInet4{
			Addr: s.addr.IP,
			Port: s.addr.Port,
		})
		if err != nil {
			if errors.Is(err, syscall.EINPROGRESS) {
				err = s.pollFdWithTimeout(s.GetFd(), s.getTimeout(), "connect")
				if err != nil {
					if err == models.ErrPollTimeout {
						return models.ErrTimeout
					}
					slog.Error("ошибка подключения сокета", "module", "socket", "addr", s.addr.Raw, "err", err)
					return err
				}
				continue
			}
			slog.Error("ошибка подключения сокета", "module", "socket", "addr", s.addr.Raw, "err", err)
			return err
		}
		break
	}

	return nil
}

func (s *socket) poll(eventType string) error {
	pUnit := models.PollingUnit{
		SocketFd: s.GetFd(),
		EventType: eventType,
		ResultChan: make(chan error),
	}
	err := s.poller.Add(pUnit)
	if err != nil {
		return err
	}

	select {
	case err = <-pUnit.ResultChan:
		if err != nil {
			return err
		}
		return nil

	case <-s.cancelChan:
		s.poller.StopUnitPolling(pUnit)

		return models.ErrPollCancelled

	}
}

func (s *socket) pollFdWithTimeout(fd int, t time.Duration, eventType string) error {
	pUnit := models.PollingUnit{
		SocketFd: fd,
		EventType: eventType,
		ResultChan: make(chan error),
	}
	err := s.poller.Add(pUnit)
	if err != nil {
		return err
	}
	timer := time.NewTimer(t)
	defer timer.Stop()
	
	select {
	case err = <-pUnit.ResultChan:
		if err != nil {
			return err
		}
		return nil

	case <-timer.C:
		s.poller.StopUnitPolling(pUnit)

		return models.ErrPollTimeout

	}
}

func (s *socket) WithCancel(cancelChan chan struct{}) {
	s.cancelChan = cancelChan
}

func (s *socket) DropCancel() {
	s.cancelChan = nil
}

func (s *socket) Readable() <-chan error {
	ch := make(chan error, 1)
	ch <- s.poll("income")
	return ch
}

func (s *socket) CopyTo(dst models.Conn) error {
	if !s.hasPipe() || s.hasDataInPipe() {
		if s.hasDataInPipe() {
			s.closePipe()
		}
		err := s.makePipe()
		if err != nil {
			return err
		}
	}

	err := s.transfer(s.GetFd(), s.getPipeWriteFd())
	if err != nil {
		return err
	}
	s.newDataInPipe()
	err = s.transfer(s.getPipeReadFd(), dst.GetFd())
	if err != nil {
		return err
	}
	s.noDataInPipe()
	
	return nil
}

func (s *socket) transfer(src, dst int) error {
	for {
		_, err := s.sys.Splice(src, dst)
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) {
				err = s.pollFdWithTimeout(dst, s.getTimeout(), "outcome")
				if err == models.ErrPollTimeout {
					if n, _ := s.CheckUnread(); n == 0 {
						return models.ErrEOF
					}
					return models.ErrTimeout
				}

				continue
			}
			return err
		}
		break
	}
	
	return nil
}

func (s *socket) Close() {
	s.poller.StopUnitPolling(models.PollingUnit{
		SocketFd: s.GetFd(),
		EventType: "income",
	})
	s.poller.StopUnitPolling(models.PollingUnit{
		SocketFd: s.GetFd(),
		EventType: "outcome",
	})
	s.sys.Close(s.GetFd())
	if s.hasPipe() {
		s.closePipe()
	}
}

func (s *socket) GetRawAddr() string {
	return s.addr.Raw
}

func (s *socket) CheckUnread() (int, error) {
	return s.sys.GetUnread(s.GetFd())
}

func (s *socket) CheckUnsent() (int, error) {
	return s.sys.GetUnsent(s.GetFd())
}

func (s *socket) GetFd() int {
	return s.fd
}

func (s *socket) SetTimeout(t time.Duration) {
	s.timeout = t
}

func (s *socket) getTimeout() time.Duration {
	return s.timeout
}

func (s *socket) getPipeWriteFd() int {
	return s.pipeWriteFd
}

func (s *socket) getPipeReadFd() int {
	return s.pipeReadFd
}

func (s *socket) hasPipe() bool {
	return s.getPipeWriteFd() != 0 && s.getPipeReadFd() != 0
}

func (s *socket) makePipe() error {
	r, w, err := s.sys.Pipe()
	if err != nil {
		return err
	}
	s.pipeReadFd = r
	s.pipeWriteFd = w

	return nil
}

func (s *socket) closePipe() {
	s.sys.Close(s.getPipeWriteFd())
	s.sys.Close(s.getPipeReadFd())
}

func (s *socket) hasDataInPipe() bool {
	return s.dataInPipe
}

func (s *socket) newDataInPipe() {
	s.dataInPipe = true
}

func (s *socket) noDataInPipe() {
	s.dataInPipe = false
}