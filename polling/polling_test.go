package polling

import (
	"context"
	"sync"
	// "errors"
	"syscall"
	"testing"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type mockSyscalls struct {
	waitFunc func(int, []syscall.EpollEvent, int) (int, error)
	getSocketOptFunc func(int, int, int) (int, error)
	ctlFunc func(int, int, int, *syscall.EpollEvent) error
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

var testPoller = &Epoll{
	fd: 2,
	mu: sync.Mutex{},
	eventsBuf: make([]syscall.EpollEvent, 5),
	readyEvents: make([]syscall.EpollEvent, 0, 5),
	sockets: make(map[int]map[string][]models.PollingUnit),
	socketsUnexpErr: make(map[int]error),
}

func TestAdd(t *testing.T) {
	testData := []struct{
		name string
		setUpFunc func()
		cleanUpFunc func()
		expectedMethodErr error
		expectedChanErr error
		expectedPollerErr error
		eventForPolling models.PollingUnit
		mockSys mockSyscalls
	}{
		{
			name: "success connect",
			setUpFunc: func() {
				testPoller.eventsBuf[0] = syscall.EpollEvent{
					Events: syscall.EPOLLOUT,
					Fd: 5,
				}
			},
			expectedMethodErr: nil,
			expectedChanErr: nil,
			expectedPollerErr: nil,

			eventForPolling: models.PollingUnit{
				SocketFd: 5,
				EventType: "connect",
				ResultChan: make(chan error),
			},
			mockSys: mockSyscalls{
				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
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
			setUpFunc: func() {
				testPoller.eventsBuf[0] = syscall.EpollEvent{
					Events: syscall.EPOLLIN,
					Fd: 5,
				}
			},
			expectedMethodErr: nil,
			expectedChanErr: nil,
			expectedPollerErr: nil,

			eventForPolling: models.PollingUnit{
				SocketFd: 5,
				EventType: "income",
				ResultChan: make(chan error),
			},
			mockSys: mockSyscalls{
				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
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
			name: "success outcome",
			setUpFunc: func() {
				testPoller.eventsBuf[0] = syscall.EpollEvent{
					Events: syscall.EPOLLOUT,
					Fd: 5,
				}
			},
			expectedMethodErr: nil,
			expectedChanErr: nil,
			expectedPollerErr: nil,

			eventForPolling: models.PollingUnit{
				SocketFd: 5,
				EventType: "outcome",
				ResultChan: make(chan error),
			},
			mockSys: mockSyscalls{
				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
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
		// 	name: "fail unexp socket err",
		// 	setUpFunc: func() {
		// 		testPoller.setSocketUnexpErr(7, models.ErrSocketEvent)
		// 	},
		// 	expectedMethodErr: models.ErrSocketEvent,
		// 	expectedChanErr: nil,
		// 	expectedPollerErr: nil,

		// 	eventForPolling: models.PollingUnit{
		// 		SocketFd: 7,
		// 		EventType: "outcome",
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
		// 	name: "fail ErrSocketAlreadyAdded",
		// 	setUpFunc: func() {
		// 		testPoller.sockets[7] = models.PollingUnit{}
		// 	},
		// 	cleanUpFunc: func() {
		// 		delete(testPoller.sockets, 7)
		// 	},
		// 	expectedMethodErr: models.ErrSocketAlreadyAdded,
		// 	expectedChanErr: nil,
		// 	expectedPollerErr: nil,

		// 	eventForPolling: models.PollingUnit{
		// 		SocketFd: 7,
		// 		EventType: "outcome",
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
		// 	name: "fail ErrPollUnknownEventType",
		// 	expectedMethodErr: models.ErrPollUnknownEventType,
		// 	expectedChanErr: nil,
		// 	expectedPollerErr: nil,

		// 	eventForPolling: models.PollingUnit{
		// 		SocketFd: 5,
		// 		EventType: "test",
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
		// 	name: "fail ctl ErrPollBadFD",
		// 	expectedMethodErr: models.ErrPollBadFD,
		// 	expectedChanErr: nil,
		// 	expectedPollerErr: nil,

		// 	eventForPolling: models.PollingUnit{
		// 		SocketFd: 5,
		// 		EventType: "connect",
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
		// 			return syscall.EBADF
		// 		},
		// 	},
		// },
	}
	
	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			testPoller.sys = &tt.mockSys

			if tt.setUpFunc != nil {
				tt.setUpFunc()
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

			if tt.cleanUpFunc != nil {
				tt.cleanUpFunc()
			}
		})
	}
}

// func Test_wait(t *testing.T) {
// 	testData := []struct{
// 		name string
// 		setUpFunc func()
// 		expectedChanErr error
// 		expectedPollerErr error
// 		eventForPolling models.PollingUnit
// 		mockSys mockSyscalls
// 	}{
// 		{
// 			name: "success",
// 			setUpFunc: func() {
// 				testPoller.eventsBuf[0] = syscall.EpollEvent{
// 					Events: syscall.EPOLLOUT,
// 					Fd: 1,
// 				}
// 			},
// 			expectedChanErr: nil,
// 			eventForPolling: models.PollingUnit{
// 				SocketFd: 1,
// 				EventType: "outcome",
// 				ResultChan: make(chan error),
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
// 			name: "fail",
// 			expectedChanErr: models.ErrPollNoMemory,
// 			eventForPolling: models.PollingUnit{
// 				SocketFd: 1,
// 				EventType: "outcome",
// 				ResultChan: make(chan error),
// 			},
// 			mockSys: mockSyscalls{
// 				waitFunc: func(_ int, _ []syscall.EpollEvent, _ int) (int, error) {
// 					return 1, models.ErrPollNoMemory
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
// 			testPoller.sys = &tt.mockSys
// 			testPoller.mu.Lock()
// 			testPoller.addSocketInPolling(tt.eventForPolling)
// 			testPoller.mu.Unlock()

// 			if tt.setUpFunc != nil {
// 				tt.setUpFunc()
// 			}

// 			testPoller.wait()
// 			err := <-tt.eventForPolling.ResultChan
// 			if err != tt.expectedChanErr {
// 				t.Errorf("Ожидаемая ошибка %s, получено %s", tt.expectedChanErr, err)
// 			}
// 		})
// 	}
// }

// func Test_processEvents(t *testing.T) {
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
// 			expectedChanErr: models.ErrSocketNoAccess,
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
// 					return 0, models.ErrSocketNoAccess
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
// 			name: "fail ErrPollDiffEventType",
// 			expectedChanErr: models.ErrPollDiffEventType,
// 			expectedPollerErr: nil,
// 			eventForPolling: models.PollingUnit{
// 				SocketFd: 4,
// 				EventType: "outcome",
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
// 			testPoller.sys = &tt.mockSys
// 			testPoller.mu.Lock()
// 			testPoller.addSocketInPolling(tt.eventForPolling)
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
// 			}
// 			cancelFunc()

// 			err := testPoller.GetError()
// 			if err != tt.expectedPollerErr {
// 				t.Errorf("Ожидаемая ошибка %s, получено %s", tt.expectedPollerErr, err)
// 			}
// 		})
// 	}
// }