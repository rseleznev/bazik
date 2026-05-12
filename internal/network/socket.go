package network

import (
	"errors"
	"runtime"
	"syscall"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type socket struct {
	fd int
	responseTimeout time.Duration

	pipeWriteFd int
	pipeReadFd int
	
	sys syscaller
	poller poller
}

func (s *socket) CopyTo(dst *socket) error {
	err := s.waitIncome()
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
func (s *socket) waitIncome() error {
	pUnit := models.PollingUnit{
		SocketFd: s.getFd(),
		EventType: "income",
		ResultChan: make(chan error),
	}
	err := s.poller.Add(pUnit)
	if err != nil {
		return err
	}

	deadline := time.Now().Add(s.getTimeout())
	for {
		select {
		case err = <-pUnit.ResultChan:
			if err != nil {
				// ?
			}
			return nil

		default:
			if time.Now().After(deadline) {
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
	return s.responseTimeout
}

func (s *socket) getPipeWriteFd() int {
	return s.pipeWriteFd
}

func (s *socket) getPipeReadFd() int {
	return s.pipeReadFd
}