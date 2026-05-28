package polling

import (
	"context"
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

func TestAddTwoEvents(t *testing.T) {
	requestCounter := 0
	testPoller := &Epoll{
		fd:              2,
		mu:              sync.Mutex{},
		eventsBuf:       make([]syscall.EpollEvent, 5),
		readyEvents:     make([]syscall.EpollEvent, 0, 5),
		sockets:         make(map[int]map[string][]models.PollingUnit),
		socketsUnexpErr: make(map[int]error),
	}
	
	testData := []struct {
		name              string
		firstEvent        models.PollingUnit
		expectedFChanErr  error
		FChanErrChecker func(error)
		secondEvent       models.PollingUnit
		expectedSChanErr  error
		SChanErrChecker func(error)
		expectedMethodErr error
		expectedPollerErr error
		mockSys           mockSyscalls
	}{
		{
			name: "success different",
			firstEvent: models.PollingUnit{
				SocketFd:   5,
				EventType:  "income",
				ResultChan: make(chan error),
			},
			expectedFChanErr: nil,
			secondEvent: models.PollingUnit{
				SocketFd:   5,
				EventType:  "outcome",
				ResultChan: make(chan error),
			},
			expectedSChanErr:  nil,
			expectedMethodErr: nil,
			expectedPollerErr: nil,
			mockSys: mockSyscalls{
				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
					time.Sleep(time.Millisecond * 300)
					testPoller.eventsBuf[0] = syscall.EpollEvent{
						Events: syscall.EPOLLIN | syscall.EPOLLOUT,
						Fd:     5,
					}
					return 1, nil
				},
				getSocketOptFunc: func(_, _, _ int) (int, error) {
					return 0, nil
				},
				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
					return nil
				},
			},
		},
		{
			name: "success same",
			firstEvent: models.PollingUnit{
				SocketFd:   5,
				EventType:  "income",
				ResultChan: make(chan error),
			},
			expectedFChanErr: nil,
			secondEvent: models.PollingUnit{
				SocketFd:   5,
				EventType:  "income",
				ResultChan: make(chan error),
			},
			expectedSChanErr:  nil,
			expectedMethodErr: nil,
			expectedPollerErr: nil,
			mockSys: mockSyscalls{
				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
					time.Sleep(time.Millisecond * 300)
					testPoller.eventsBuf[0] = syscall.EpollEvent{
						Events: syscall.EPOLLIN,
						Fd:     5,
					}
					return 1, nil
				},
				getSocketOptFunc: func(_, _, _ int) (int, error) {
					return 0, nil
				},
				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
					return nil
				},
			},
		},
		{
			name: "success only one ready",
			firstEvent: models.PollingUnit{
				SocketFd:   5,
				EventType:  "income",
				ResultChan: make(chan error),
			},
			expectedFChanErr: nil,
			secondEvent: models.PollingUnit{
				SocketFd:   5,
				EventType:  "outcome",
				ResultChan: make(chan error),
			},
			expectedSChanErr:  nil,
			expectedMethodErr: nil,
			expectedPollerErr: nil,
			mockSys: mockSyscalls{
				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
					if requestCounter == 0 {
						time.Sleep(time.Millisecond * 300)
						requestCounter++
						testPoller.eventsBuf[0] = syscall.EpollEvent{
							Events: syscall.EPOLLIN,
							Fd:     5,
						}
						return 1, nil
					}
					return 0, nil
				},
				getSocketOptFunc: func(_, _, _ int) (int, error) {
					return 0, nil
				},
				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
					return nil
				},
			},
		},
		{
			name: "two sockets (one success, one fail)",
			firstEvent: models.PollingUnit{
				SocketFd:   5,
				EventType:  "income",
				ResultChan: make(chan error),
			},
			expectedFChanErr: models.ErrSocketHUPEvent,
			FChanErrChecker: func(err error) {
				if !errors.Is(err, models.ErrSocketHUPEvent) {
					t.Errorf("Ожидаемая ошибка %s, получено %s", models.ErrSocketHUPEvent, err)
				}
			},
			secondEvent: models.PollingUnit{
				SocketFd:   6,
				EventType:  "income",
				ResultChan: make(chan error),
			},
			expectedSChanErr:  nil,
			expectedMethodErr: nil,
			expectedPollerErr: nil,
			mockSys: mockSyscalls{
				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
					time.Sleep(time.Millisecond * 300)
					testPoller.eventsBuf[0] = syscall.EpollEvent{
						Events: syscall.EPOLLHUP,
						Fd: 5,
					}
					testPoller.eventsBuf[1] = syscall.EpollEvent{
						Events: syscall.EPOLLIN,
						Fd: 6,
					}
					return 2, nil
				},
				getSocketOptFunc: func(_, _, _ int) (int, error) {
					return 0, nil
				},
				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
					return nil
				},
			},
		},
		{
			name: "fail common",
			firstEvent: models.PollingUnit{
				SocketFd:   5,
				EventType:  "income",
				ResultChan: make(chan error),
			},
			expectedFChanErr: models.ErrSocketHUPEvent,
			FChanErrChecker: func(err error) {
				if !errors.Is(err, models.ErrSocketHUPEvent) {
					t.Errorf("Ожидаемая ошибка %s, получено %s", models.ErrSocketHUPEvent, err)
				}
			},
			secondEvent: models.PollingUnit{
				SocketFd:   5,
				EventType:  "outcome",
				ResultChan: make(chan error),
			},
			expectedSChanErr:  models.ErrSocketHUPEvent,
			SChanErrChecker: func(err error) {
				if !errors.Is(err, models.ErrSocketHUPEvent) {
					t.Errorf("Ожидаемая ошибка %s, получено %s", models.ErrSocketHUPEvent, err)
				}
			},
			expectedMethodErr: nil,
			expectedPollerErr: nil,
			mockSys: mockSyscalls{
				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
					time.Sleep(time.Millisecond * 300)
					testPoller.eventsBuf[0] = syscall.EpollEvent{
						Events: syscall.EPOLLHUP,
						Fd: 5,
					}
					return 1, nil
				},
				getSocketOptFunc: func(_, _, _ int) (int, error) {
					return 0, nil
				},
				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
					return nil
				},
			},
		},
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			testPoller.mu.Lock()
			testPoller.sys = &tt.mockSys
			testPoller.mu.Unlock()

			err := testPoller.Add(tt.firstEvent)
			if err != tt.expectedMethodErr {
				t.Errorf("Ожидаемая ошибка %s, получено %s", tt.expectedMethodErr, err)
			}
			err = testPoller.Add(tt.secondEvent)
			if err != tt.expectedMethodErr {
				t.Errorf("Ожидаемая ошибка %s, получено %s", tt.expectedMethodErr, err)
			}

			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*1)

			for {
				select {
				case err = <-tt.firstEvent.ResultChan:
					if tt.FChanErrChecker != nil {
						tt.FChanErrChecker(err)
					} else {
						if err != tt.expectedFChanErr {
							t.Errorf("Ожидаемая ошибка %s, получено %s", tt.expectedFChanErr, err)
						}	
					}
					tt.firstEvent.ResultChan = nil
					if tt.secondEvent.ResultChan == nil {
						break
					}
					continue

				case err = <-tt.secondEvent.ResultChan:
					if tt.SChanErrChecker != nil {
						tt.SChanErrChecker(err)
					} else {
						if err != tt.expectedSChanErr {
							t.Errorf("Ожидаемая ошибка %s, получено %s", tt.expectedSChanErr, err)
						}	
					}
					tt.secondEvent.ResultChan = nil
					if tt.firstEvent.ResultChan == nil {
						break
					}
					continue

				case <-ctx.Done():
					t.Log("Вышли из select по таймауту")
					testPoller.DeleteSocketFromPolling(5)

				}
				break
			}
			cancelFunc()

			err = testPoller.GetError()
			if err != tt.expectedPollerErr {
				t.Errorf("Ожидаемая ошибка %s, получено %s", tt.expectedPollerErr, err)
			}
		})
	}
}