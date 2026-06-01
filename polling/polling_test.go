package polling

import (
	"context"
	// "errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type mockSyscalls struct {
	waitFunc         func(int, []syscall.EpollEvent, int) (int, error)
	getSocketOptFunc func(int, int, int) (int, error)
	ctlFunc          func(int, int, int, *syscall.EpollEvent) error
}

func (ms *mockSyscalls) Wait(eFd int, events []syscall.EpollEvent, timeout int) (int, error) {
	return ms.waitFunc(eFd, events, timeout)
}

func (ms *mockSyscalls) GetSocketOpt(sFd, l, o int) (int, error) {
	return ms.getSocketOptFunc(sFd, l, o)
}

func (ms *mockSyscalls) Ctl(eFd, o, sFd int, event *syscall.EpollEvent) error {
	return ms.ctlFunc(eFd, o, sFd, event)
}


func TestAdd(t *testing.T) {
	testPoller := &Epoll{
		fd:              2,
		mu:              sync.Mutex{},
		eventsBuf:       make([]syscall.EpollEvent, 5),
		readyEvents:     make([]syscall.EpollEvent, 0, 5),
		interestedSockets: make(map[int]struct{}),
		sockets:         make(map[int]map[string][]models.PollingUnit),
		socketsUnexpErr: make(map[int]error),
	}
	
	testData := []struct {
		name              string
		setupFunc         func()
		expectedMethodErr error
		expectedChanErr   error
		expectedPollerErr error
		eventForPolling   models.PollingUnit
		mockSys           mockSyscalls
	}{
		{
			name: "success connect",
			expectedMethodErr: nil,
			expectedChanErr:   nil,
			expectedPollerErr: nil,

			eventForPolling: models.PollingUnit{
				SocketFd:   5,
				EventType:  "connect",
				ResultChan: make(chan error),
			},
			mockSys: mockSyscalls{
				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
					testPoller.eventsBuf[0] = syscall.EpollEvent{
						Events: syscall.EPOLLOUT,
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
			name: "success income",
			expectedMethodErr: nil,
			expectedChanErr:   nil,
			expectedPollerErr: nil,

			eventForPolling: models.PollingUnit{
				SocketFd:   5,
				EventType:  "income",
				ResultChan: make(chan error),
			},
			mockSys: mockSyscalls{
				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
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
		// {
		// 	name: "success outcome",
		// 	expectedMethodErr: nil,
		// 	expectedChanErr:   nil,
		// 	expectedPollerErr: nil,

		// 	eventForPolling: models.PollingUnit{
		// 		SocketFd:   5,
		// 		EventType:  "outcome",
		// 		ResultChan: make(chan error),
		// 	},
		// 	mockSys: mockSyscalls{
		// 		waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
		// 			testPoller.eventsBuf[0] = syscall.EpollEvent{
		// 				Events: syscall.EPOLLOUT,
		// 				Fd:     5,
		// 			}
		// 			return 1, nil
		// 		},
		// 		getSocketOptFunc: func(_, _, _ int) (int, error) {
		// 			return 0, nil
		// 		},
		// 		ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
		// 			return nil
		// 		},
		// 	},
		// },
		// {
		// 	name: "fail unexp socket err",
		// 	setupFunc: func() {
		// 		testPoller.setSocketUnexpErr(7, models.ErrSocketEvent)
		// 	},
		// 	expectedMethodErr: models.ErrSocketEvent,
		// 	expectedChanErr:   nil,
		// 	expectedPollerErr: nil,

		// 	eventForPolling: models.PollingUnit{
		// 		SocketFd:   7,
		// 		EventType:  "outcome",
		// 		ResultChan: make(chan error),
		// 	},
		// 	mockSys: mockSyscalls{
		// 		waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
		// 			return 1, nil
		// 		},
		// 		getSocketOptFunc: func(_, _, _ int) (int, error) {
		// 			return 0, nil
		// 		},
		// 		ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
		// 			return nil
		// 		},
		// 	},
		// },
		// {
		// 	name:              "fail ErrPollUnknownEventType",
		// 	expectedMethodErr: models.ErrPollUnknownEventType,
		// 	expectedChanErr:   nil,
		// 	expectedPollerErr: nil,

		// 	eventForPolling: models.PollingUnit{
		// 		SocketFd:   5,
		// 		EventType:  "test",
		// 		ResultChan: make(chan error),
		// 	},
		// 	mockSys: mockSyscalls{
		// 		waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
		// 			return 1, nil
		// 		},
		// 		getSocketOptFunc: func(_, _, _ int) (int, error) {
		// 			return 0, nil
		// 		},
		// 		ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
		// 			return nil
		// 		},
		// 	},
		// },
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			testPoller.sys = &tt.mockSys

			if tt.setupFunc != nil {
				tt.setupFunc()
			}

			err := testPoller.Add(tt.eventForPolling)
			if err != tt.expectedMethodErr {
				t.Errorf("Ожидаемая ошибка %s, получено %s", tt.expectedMethodErr, err)
			}

			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*1)

			select {
			case err = <-tt.eventForPolling.ResultChan:
				if err != tt.expectedChanErr {
					t.Errorf("Ожидаемая ошибка %s, получено %s", tt.expectedChanErr, err)
				}

			case <-ctx.Done():
				t.Log("Вышли из select по таймауту")

			}
			cancelFunc()

			err = testPoller.GetError()
			if err != tt.expectedPollerErr {
				t.Errorf("Ожидаемая ошибка %s, получено %s", tt.expectedPollerErr, err)
			}
		})
	}
}

// func Test_wait(t *testing.T) {
// 	testPoller := &Epoll{
// 		fd:              2,
// 		mu:              sync.Mutex{},
// 		eventsBuf:       make([]syscall.EpollEvent, 5),
// 		readyEvents:     make([]syscall.EpollEvent, 0, 5),
// 		sockets:         make(map[int]map[string][]models.PollingUnit),
// 		socketsUnexpErr: make(map[int]error),
// 	}
	
// 	testData := []struct{
// 		name string
// 		expectedChanErr error
// 		expectedPollerErr error
// 		eventForPolling models.PollingUnit
// 		mockSys mockSyscalls
// 	}{
// 		{
// 			name: "success",
// 			expectedChanErr: nil,
// 			eventForPolling: models.PollingUnit{
// 				SocketFd: 1,
// 				EventType: "outcome",
// 				ResultChan: make(chan error),
// 			},
// 			mockSys: mockSyscalls{
// 				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
// 					testPoller.eventsBuf[0] = syscall.EpollEvent{
// 						Events: syscall.EPOLLOUT,
// 						Fd: 1,
// 					}
// 					return 1, nil
// 				},
// 				getSocketOptFunc: func(_, _, _ int) (int, error) {
// 					return 0, nil
// 				},
// 				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
// 					return nil
// 				},
// 			},
// 		},
// 		{
// 			name: "fail",
// 			expectedChanErr: models.ErrWrongProto,
// 			eventForPolling: models.PollingUnit{
// 				SocketFd: 1,
// 				EventType: "outcome",
// 				ResultChan: make(chan error),
// 			},
// 			mockSys: mockSyscalls{
// 				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
// 					return 1, models.ErrWrongProto
// 				},
// 				getSocketOptFunc: func(_, _, _ int) (int, error) {
// 					return 0, nil
// 				},
// 				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
// 					return nil
// 				},
// 			},
// 		},
// 	}

// 	for _, tt := range testData {
// 		t.Run(tt.name, func(t *testing.T) {
// 			testPoller.mu.Lock()
// 			testPoller.sys = &tt.mockSys
// 			testPoller.sockets[tt.eventForPolling.SocketFd] = make(map[string][]models.PollingUnit, 2)
// 			testPoller.addSocketEventInPolling(tt.eventForPolling)
// 			testPoller.mu.Unlock()

// 			testPoller.wait()
// 			err := <-tt.eventForPolling.ResultChan
// 			if err != tt.expectedChanErr {
// 				t.Errorf("Ожидаемая ошибка %s, получено %s", tt.expectedChanErr, err)
// 			}
// 		})
// 	}
// }

// func Test_processEvents(t *testing.T) {
// 	testPoller := &Epoll{
// 		fd:              2,
// 		mu:              sync.Mutex{},
// 		eventsBuf:       make([]syscall.EpollEvent, 5),
// 		readyEvents:     make([]syscall.EpollEvent, 0, 5),
// 		sockets:         make(map[int]map[string][]models.PollingUnit),
// 		socketsUnexpErr: make(map[int]error),
// 	}
	
// 	testData := []struct{
// 		name string
// 		expectedChanErr error
// 		expectedPollerErr error
// 		eventForPolling models.PollingUnit
// 		readyEvents []syscall.EpollEvent
// 		mockSys mockSyscalls
// 	}{
// 		{
// 			name: "success connect",
// 			expectedChanErr: nil,
// 			expectedPollerErr: nil,
// 			eventForPolling: models.PollingUnit{
// 				SocketFd: 4,
// 				EventType: "connect",
// 				ResultChan: make(chan error),
// 			},
// 			readyEvents: []syscall.EpollEvent{
// 				{
// 					Events: syscall.EPOLLOUT,
// 					Fd: 4,
// 				},
// 			},
// 			mockSys: mockSyscalls{
// 				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
// 					return 1, nil
// 				},
// 				getSocketOptFunc: func(_, _, _ int) (int, error) {
// 					return 0, nil
// 				},
// 				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
// 					return nil
// 				},
// 			},
// 		},
// 		{
// 			name: "success income",
// 			expectedChanErr: nil,
// 			expectedPollerErr: nil,
// 			eventForPolling: models.PollingUnit{
// 				SocketFd: 4,
// 				EventType: "income",
// 				ResultChan: make(chan error),
// 			},
// 			readyEvents: []syscall.EpollEvent{
// 				{
// 					Events: syscall.EPOLLIN,
// 					Fd: 4,
// 				},
// 			},
// 			mockSys: mockSyscalls{
// 				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
// 					return 1, nil
// 				},
// 				getSocketOptFunc: func(_, _, _ int) (int, error) {
// 					return 0, nil
// 				},
// 				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
// 					return nil
// 				},
// 			},
// 		},
// 		{
// 			name: "success outcome",
// 			expectedChanErr: nil,
// 			expectedPollerErr: nil,
// 			eventForPolling: models.PollingUnit{
// 				SocketFd: 4,
// 				EventType: "outcome",
// 				ResultChan: make(chan error),
// 			},
// 			readyEvents: []syscall.EpollEvent{
// 				{
// 					Events: syscall.EPOLLOUT,
// 					Fd: 4,
// 				},
// 			},
// 			mockSys: mockSyscalls{
// 				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
// 					return 1, nil
// 				},
// 				getSocketOptFunc: func(_, _, _ int) (int, error) {
// 					return 0, nil
// 				},
// 				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
// 					return nil
// 				},
// 			},
// 		},
// 		{
// 			name: "fail socketOpt",
// 			expectedChanErr: models.ErrWrongProto,
// 			expectedPollerErr: nil,
// 			eventForPolling: models.PollingUnit{
// 				SocketFd: 4,
// 				EventType: "outcome",
// 				ResultChan: make(chan error),
// 			},
// 			readyEvents: []syscall.EpollEvent{
// 				{
// 					Events: syscall.EPOLLOUT,
// 					Fd: 4,
// 				},
// 			},
// 			mockSys: mockSyscalls{
// 				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
// 					return 1, nil
// 				},
// 				getSocketOptFunc: func(_, _, _ int) (int, error) {
// 					return 0, models.ErrWrongProto
// 				},
// 				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
// 					return nil
// 				},
// 			},
// 		},
// 		{
// 			name: "fail event EPOLLERR",
// 			expectedChanErr: models.ErrSocketEvent,
// 			expectedPollerErr: nil,
// 			eventForPolling: models.PollingUnit{
// 				SocketFd: 4,
// 				EventType: "outcome",
// 				ResultChan: make(chan error),
// 			},
// 			readyEvents: []syscall.EpollEvent{
// 				{
// 					Events: syscall.EPOLLERR,
// 					Fd: 4,
// 				},
// 			},
// 			mockSys: mockSyscalls{
// 				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
// 					return 1, nil
// 				},
// 				getSocketOptFunc: func(_, _, _ int) (int, error) {
// 					return 0, nil
// 				},
// 				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
// 					return nil
// 				},
// 			},
// 		},
// 		{
// 			name: "fail event EPOLLHUP",
// 			expectedChanErr: models.ErrSocketHUPEvent,
// 			expectedPollerErr: nil,
// 			eventForPolling: models.PollingUnit{
// 				SocketFd: 4,
// 				EventType: "outcome",
// 				ResultChan: make(chan error),
// 			},
// 			readyEvents: []syscall.EpollEvent{
// 				{
// 					Events: syscall.EPOLLHUP,
// 					Fd: 4,
// 				},
// 			},
// 			mockSys: mockSyscalls{
// 				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
// 					return 1, nil
// 				},
// 				getSocketOptFunc: func(_, _, _ int) (int, error) {
// 					return 0, nil
// 				},
// 				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
// 					return nil
// 				},
// 			},
// 		},
// 		{
// 			name: "fail event EPOLLRDHUP",
// 			expectedChanErr: models.ErrSocketRDHUPEvent,
// 			expectedPollerErr: nil,
// 			eventForPolling: models.PollingUnit{
// 				SocketFd: 4,
// 				EventType: "outcome",
// 				ResultChan: make(chan error),
// 			},
// 			readyEvents: []syscall.EpollEvent{
// 				{
// 					Events: syscall.EPOLLRDHUP,
// 					Fd: 4,
// 				},
// 			},
// 			mockSys: mockSyscalls{
// 				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
// 					return 1, nil
// 				},
// 				getSocketOptFunc: func(_, _, _ int) (int, error) {
// 					return 0, nil
// 				},
// 				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
// 					return nil
// 				},
// 			},
// 		},
// 		{
// 			name: "fail nilResultChan",
// 			expectedChanErr: nil,
// 			expectedPollerErr: nil,
// 			eventForPolling: models.PollingUnit{
// 				SocketFd: 4,
// 				EventType: "outcome",
// 				ResultChan: nil,
// 			},
// 			readyEvents: []syscall.EpollEvent{
// 				{
// 					Events: syscall.EPOLLIN,
// 					Fd: 4,
// 				},
// 			},
// 			mockSys: mockSyscalls{
// 				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
// 					return 1, nil
// 				},
// 				getSocketOptFunc: func(_, _, _ int) (int, error) {
// 					return 0, nil
// 				},
// 				ctlFunc: func(_, _, _ int, _ *syscall.EpollEvent) error {
// 					return nil
// 				},
// 			},
// 		},
// 	}

// 	for _, tt := range testData {
// 		t.Run(tt.name, func(t *testing.T) {
// 			testPoller.mu.Lock()
// 			testPoller.sys = &tt.mockSys
// 			testPoller.sockets[tt.eventForPolling.SocketFd] = make(map[string][]models.PollingUnit, 2)
// 			testPoller.addSocketEventInPolling(tt.eventForPolling)
// 			testPoller.addReadyEvents(tt.readyEvents)
// 			testPoller.mu.Unlock()

// 			go testPoller.processEvents(1)

// 			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*1)

// 			select {
// 			case err := <-tt.eventForPolling.ResultChan:
// 				if !errors.Is(err, tt.expectedChanErr) {
// 					t.Errorf("Ожидаемая ошибка %s, получено %s", tt.expectedChanErr, err)
// 				}

// 			case <-ctx.Done():
// 				t.Log("Вышли из select по таймауту")
// 				testPoller.DeleteSocketFromPolling(tt.eventForPolling.SocketFd)
// 			}
// 			cancelFunc()

// 			err := testPoller.GetError()
// 			if err != tt.expectedPollerErr {
// 				t.Errorf("Ожидаемая ошибка %s, получено %s", tt.expectedPollerErr, err)
// 			}
// 		})
// 	}
// }