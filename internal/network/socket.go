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
	timeout time.Duration
	idleDeadline time.Time

	logActivity bool
	lastActivity time.Time

	pipeWriteFd int
	pipeReadFd int
	
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

func (s *socket) CopyTo(dst *socket) error {
	err := s.poll("income")
	if s.isLogActivity() {
		s.updateLastActivity()
	}
	if err != nil {
		return err // ?
	}

	err = s.transfer(s.getFd(), s.getPipeWriteFd())
	if err != nil {
		return err
	}

	err = s.transfer(s.getPipeReadFd(), dst.getFd())
	if err != nil {
		return err
	}
	
	return nil
}

// переделать на функцию вместо метода?
func (s *socket) poll(eventType string) error {
	pUnit := models.PollingUnit{
		SocketFd: s.getFd(),
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
				// ?
			}
			return nil

		default:
			if time.Now().After(s.getIdleDeadline()) {
				s.poller.DeleteSocketFromPolling(s.getFd())

				return models.ErrResponseTimeout
			}
			runtime.Gosched()
			continue

		}
	}
}

func (s *socket) transfer(src, dst int) error {
	for {
		_, err := s.sys.Splice(src, dst)
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) {
				// poll

				continue
			}
			return err
		}
		break
	}
	
	return nil
}

func (s *socket) getFd() int {
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