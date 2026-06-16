package polling

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rseleznev/bazik/internal/models"
	"golang.org/x/sys/unix"
)

const (
	epollIncomeEvent = 0x01
	epollOutcomeEvent = 0x02
)

type Epoll struct {
	// файловый дескриптор инстанса epoll
	fd int
	// счетчик ждущих потоков
	waitersCounter atomic.Int32

	mu sync.RWMutex
	err error

	// флаг, запущен ли поллинг
	polling bool

	// буфер для готовых событий
	eventsBuf []syscall.EpollEvent

	// события, которые нужно обработать
	readyEvents []syscall.EpollEvent

	// сокеты, которые поллим, события и каналы для возврата результата
	socketsPolling map[int]map[string][]models.PollingUnit
	
	// пришло неожиданное событие с ошибкой, когда никто не ждал
	socketsUnexpErr map[int]error

	// интерфейс системных вызовов
	sys epollSyscalls
}

func NewEpoll() (*Epoll, error) {
	eFd, err := syscall.EpollCreate(1)
	if err != nil {		
		return nil, fmt.Errorf("polling creation err: %w", err)
	}

	return &Epoll{
		fd: eFd,
		mu: sync.RWMutex{},
		eventsBuf: make([]syscall.EpollEvent, 5),
		readyEvents: make([]syscall.EpollEvent, 0, 5),
		socketsPolling: make(map[int]map[string][]models.PollingUnit),
		socketsUnexpErr: make(map[int]error),
		sys: epollRealSyscalls{},
	}, nil
}

// Add добавляет событие (юнит), которое нужно поллить
func (e *Epoll) Add(unit models.PollingUnit) error {
	if err := e.getSocketUnexpErr(unit.SocketFd); err != nil {
		e.deleteSocketUnexpErr(unit.SocketFd)
		return err
	}
	if !e.checkEventType(unit.EventType) {
		return models.ErrPollUnknownEventType
	}
	if !e.isSocketPolling(unit.SocketFd) {
		err := e.addCommonEvent(unit.SocketFd)
		if err != nil {
			//игнорируем ошибку, когда fd уже в interest_list
			if !errors.Is(err, syscall.EEXIST) {
				return err
			}
		}
	}
	e.addSocketUnit(unit)
	e.initPoll()

	return nil
}

// poll делает системный вызов epoll_wait и обрабатывает полученные события
//
// Крутится, пока счетчик ожидателей не опустится до 0
func (e *Epoll) poll() {
	defer e.stopPolling()
wait:
	startTime := time.Now()
	waitTypeIdentifier := 0
	for {
		n, err := e.sys.Wait(e.fd, e.eventsBuf, waitTypeIdentifier)
		if err != nil {
			// игнорируем сигнал прерывания
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			e.setError(err)
			e.pushError()
			return
		}
		if n > 0 {
			e.addReadyEvents(e.eventsBuf[:n])
			e.processEvents(n)
			break
		}
		if time.Now().After(startTime.Add(time.Millisecond*50)) {
			waitTypeIdentifier = -1
		}
		runtime.Gosched()
	}

	if e.waitersCounter.Load() > 0 {
		goto wait
	}
}

// processEvents обрабатывает полученные события и возвращает результаты ждущим потокам
func (e *Epoll) processEvents(readySocketsLen int) {
	readySockets := make(map[int]models.PollingResult, readySocketsLen)

	for _, v := range e.readyEvents {
		var errs []error
		socketFd := int(v.Fd)

		// пытаемся получить ошибку по сокету
		_, err := e.sys.GetSocketOpt(socketFd, syscall.SOL_SOCKET, syscall.SO_ERROR)
		if err != nil {
			errs = append(errs, err)
		}

		// Проверяем полученное событие
		// проверяем на наличие событий с ошибками
		if v.Events & syscall.EPOLLERR != 0 { // ошибка
			errs = append(errs, models.ErrSocketEvent)
		}
		if v.Events & syscall.EPOLLHUP != 0 { // соединение закрыто сервером
			errs = append(errs, models.ErrSocketHUPEvent)
		}
		if v.Events & syscall.EPOLLRDHUP != 0 { // сервер закрыл запись
			errs = append(errs, models.ErrSocketRDHUPEvent)
		}

		// если по сокету есть ошибки, группируем их и закидываем в результирующий словарь
		if len(errs) > 0 {
			readySockets[socketFd] = models.PollingResult{
				Err: errors.Join(errs...),
			}

			continue
		}

		// проверяем корректные события
		if v.Events & syscall.EPOLLIN != 0 { // есть данные в буфере получения
			e := readySockets[socketFd].EventType | epollIncomeEvent
			readySockets[socketFd] = models.PollingResult{
				EventType: e,
			}
		}
		if v.Events & syscall.EPOLLOUT != 0 { // буфер отправки пуст
			e := readySockets[socketFd].EventType | epollOutcomeEvent
			readySockets[socketFd] = models.PollingResult{
				EventType: e,
			}
		}
	}

	// если произошла некая рассинхронизация или странная ситуация. Такого не должно происходить
	if len(readySockets) != readySocketsLen {
		panic("not all expected sockets are ready")
	}

	// возвращаем результаты, ждущие потоки могут продолжить свое выполнение
	for s, v := range readySockets {
		if v.Err != nil {
			e.handleSocketErr(s, v.Err)
			continue
		}
		if v.EventType & epollIncomeEvent != 0 {
			e.handleSocketEvent(s, "income", v.Err)
		}
		if v.EventType & epollOutcomeEvent != 0 {
			e.handleSocketEvent(s, "outcome", v.Err)
			e.handleSocketEvent(s, "connect", v.Err)
		}
		e.socketFin(s)
	}
	e.clearReadyEvents() // удаляем завершенные события
}

func (e *Epoll) initPoll() {
	e.mu.Lock()
	if !e.isPolling() {
		e.startPolling()
		go e.poll()
	}
	e.mu.Unlock()
}

func (e *Epoll) stopPolling() {
	e.mu.Lock()
	e.polling = false
	e.mu.Unlock()
}

func (e *Epoll) StopUnitPolling(unit models.PollingUnit) {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := int32(len(e.socketsPolling[unit.SocketFd][unit.EventType]))
	e.waitersCounter.Add(-n)
	e.deleteSocketEvent(unit.SocketFd, unit.EventType) // работает только при одном ждущем юните
	if e.socketEventsLen(unit.SocketFd) == 0 {
		e.deleteSocketFromPolling(unit.SocketFd)
	}
}

func (e *Epoll) setError(err error) {
	e.err = err
}

func (e *Epoll) deleteError() {
	e.err = nil
}

func (e *Epoll) GetError() error {
	err := e.err
	e.deleteError()

	return err
}

// pushError информирует все ждущие потоки о глобальной ошибке epoll
func (e *Epoll) pushError() {
	e.mu.Lock()
	defer e.mu.Unlock()

	err := e.GetError()

	for s := range e.socketsPolling {
		e.sendResultToUnits(s, err)
	}
}

func (e *Epoll) getSocketUnexpErr(socketFd int) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.socketsUnexpErr[socketFd]
}

func (e *Epoll) deleteSocketUnexpErr(socketFd int) {
	e.mu.Lock()
	delete(e.socketsUnexpErr, socketFd)
	e.mu.Unlock()
}

func (e *Epoll) handleSocketErr(socketFd int, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.socketEventsLen(socketFd) == 0 {
		e.deleteSocketFromPolling(socketFd)
		e.setSocketUnexpErr(socketFd, err)
		return
	}
	e.sendResultToUnits(socketFd, err)
	e.deleteSocketFromPolling(socketFd)
}

func (e *Epoll) handleSocketEvent(socketFd int, eventType string, err error) {
	e.mu.Lock()
	e.sendResultToEventUnits(socketFd, eventType, err)
	e.deleteSocketEvent(socketFd, eventType)
	e.mu.Unlock()
}

func (e *Epoll) socketFin(socketFd int) {
	e.mu.Lock()
	if e.socketEventsLen(socketFd) == 0 {
		e.deleteSocketFromPolling(socketFd)
	}
	e.mu.Unlock()
}

func (e *Epoll) isSocketPolling(socketFd int) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	_, ok := e.socketsPolling[socketFd]
	return ok
}

func (e *Epoll) addSocketUnit(unit models.PollingUnit) {
	e.mu.Lock()
	if e.socketsPolling[unit.SocketFd] == nil {
		e.socketsPolling[unit.SocketFd] = make(map[string][]models.PollingUnit, 2)
		e.socketsPolling[unit.SocketFd][unit.EventType] = make([]models.PollingUnit, 0, 2)
	}
	e.socketsPolling[unit.SocketFd][unit.EventType] = append(e.socketsPolling[unit.SocketFd][unit.EventType], unit)
	e.waitersCounter.Add(1)
	e.mu.Unlock()
}

func (e *Epoll) checkEventType(eventType string) bool {
	var ok bool

	if eventType == "connect" {
		ok = true
	}
	if eventType == "income" {
		ok = true
	}
	if eventType == "outcome" {
		ok = true
	}
	return ok
}


// ------------------------------------------------
// Методы, которые должны вызываться только под захваченным мьютексом

func (e *Epoll) isPolling() bool {
	return e.polling
}

func (e *Epoll) startPolling() {
	e.polling = true
}

func (e *Epoll) socketEventsLen(socketFd int) int {
	return len(e.socketsPolling[socketFd])
}

func (e *Epoll) deleteSocketEvent(socketFd int, eventType string) {
	delete(e.socketsPolling[socketFd], eventType)
}

func (e *Epoll) deleteSocketFromPolling(socketFd int) {
	delete(e.socketsPolling, socketFd)
}

func (e *Epoll) addCommonEvent(socketFd int) error {
	err := e.sys.Ctl(e.fd, syscall.EPOLL_CTL_ADD, socketFd, &syscall.EpollEvent{
		Events: syscall.EPOLLIN | syscall.EPOLLOUT | unix.EPOLLET,
		Fd: int32(socketFd),
	})
	if err != nil {
		return err
	}
	
	return nil
}

// func (e *Epoll) deleteCommonEvent(socketFd int) error {
// 	err := e.sys.Ctl(e.fd, syscall.EPOLL_CTL_DEL, socketFd, &syscall.EpollEvent{})
// 	if err != nil {
// 		return err
// 	}
	
// 	return nil
// }

func (e *Epoll) sendResultToUnits(socketFd int, err error)  {
	for _, units := range e.socketsPolling[socketFd] {
		for _, unit := range units {
			if unit.ResultChan == nil {
				e.waitersCounter.Add(-1)
				continue
			}
			unit.ResultChan <- err
			e.waitersCounter.Add(-1)
		}
	}
}

func (e *Epoll) sendResultToEventUnits(socketFd int, eventType string, err error) {
	for _, unit := range e.socketsPolling[socketFd][eventType] {
		if unit.ResultChan == nil {
			e.waitersCounter.Add(-1)
			continue
		}
		unit.ResultChan <- err
		e.waitersCounter.Add(-1)
	}
}

func (e *Epoll) addReadyEvents(events []syscall.EpollEvent) {
	e.readyEvents = append(e.readyEvents, events...)
}

func (e *Epoll) clearReadyEvents() {
	e.readyEvents = e.readyEvents[:0]
}

func (e *Epoll) setSocketUnexpErr(socketFd int, err error) {
	e.socketsUnexpErr[socketFd] = err
}