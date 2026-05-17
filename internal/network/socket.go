package network

import (
	"errors"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type socket struct {
	fd int
	mu sync.RWMutex
	addr models.Address
	timeout time.Duration
	idleDeadline time.Time

	logActivity bool
	lastActivity time.Time

	pipeWriteFd int
	pipeReadFd int
	dataInPipe bool
	
	sys syscaller
	poller poller
}

func (s *socket) LogActivity() {
	s.logActivity = true
	s.updateLastActivity()
}

func (s *socket) LastActivity() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastActivity
}

func (s *socket) SetLastActivity(t time.Time) {
	s.mu.Lock()
	s.lastActivity = t
	s.mu.Unlock()
}

func (s *socket) SetIdleDeadline(t time.Time) {
	s.mu.Lock()
	s.idleDeadline = t
	s.mu.Unlock()
}

func (s *socket) getIdleDeadline() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.idleDeadline
}

func (s *socket) updateLastActivity() {
	s.mu.Lock()
	s.lastActivity = time.Now()
	s.mu.Unlock()
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

	for {
		select {
		case err = <-pUnit.ResultChan:
			if err != nil {
				return err
			}
			return nil

		default:
			if time.Now().After(s.getIdleDeadline()) {
				s.poller.DeleteSocketFromPolling(s.GetFd())

				return models.ErrPollTimeout
			}
			runtime.Gosched()
			continue

		}
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

	deadline := time.Now().Add(t)
	for {
		select {
		case err = <-pUnit.ResultChan:
			if err != nil {
				return err
			}
			return nil

		default:
			if time.Now().After(deadline) {
				s.poller.DeleteSocketFromPolling(fd)

				return models.ErrPollTimeout
			}
			runtime.Gosched()
			continue

		}
	}
}

func (s *socket) Accept() models.Conn
func (s *socket) Connect() error

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
	
	err := s.poll("income")
	if err != nil {
		if err == models.ErrPollTimeout {
			return models.ErrIdleTimeout
		}
		if s.isLogActivity() {
			s.updateLastActivity()
		}
		
		return err
	}
	if s.isLogActivity() {
		s.updateLastActivity()
	}

	err = s.transfer(s.GetFd(), s.getPipeWriteFd())
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
	s.sys.Close(s.GetFd())
	s.sys.Close(s.getPipeWriteFd())
	s.sys.Close(s.getPipeReadFd())
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

func (s *socket) getTimeout() time.Duration {
	return s.timeout
}

func (s *socket) getPipeWriteFd() int {
	return s.pipeWriteFd
}

func (s *socket) getPipeReadFd() int {
	return s.pipeReadFd
}

func (s *socket) isLogActivity() bool {
	return s.logActivity
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