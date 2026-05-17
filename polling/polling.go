package polling

import (
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/rseleznev/bazik/internal/models"
)

type Epoll struct {
	// файловый дескриптор инстанса epoll
	fd int

	mu sync.Mutex
	err error

	// флаг, запущен ли поллинг
	polling bool

	// буфер для готовых событий
	eventsBuf []syscall.EpollEvent

	// события, которые нужно обработать
	readyEvents []syscall.EpollEvent

	// сокеты, которые поллим и канал для возврата результата
	sockets map[int]models.PollingUnit
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
		mu: sync.Mutex{},
		eventsBuf: make([]syscall.EpollEvent, 5),
		readyEvents: make([]syscall.EpollEvent, 0, 5),
		sockets: make(map[int]models.PollingUnit),
		socketsUnexpErr: make(map[int]error),
		sys: epollRealSyscalls{},
	}, nil
}

// Add добавляет событие (юнит), которое нужно поллить
func (e *Epoll) Add(unit models.PollingUnit) error {
	e.mu.Lock()

	defer e.mu.Unlock()

	// проверка, нет ли неожиданной ошибки по сокету
	if err := e.getSocketUnexpErr(unit.SocketFd); err != nil {
		e.deleteSocketUnexpErr(unit.SocketFd)
		return err
	}

	// проверка, нет ли у нас переданного сокета в обработке
	if e.isSocketInPolling(unit.SocketFd) {
		return models.ErrSocketAlreadyAdded // вызывающий поток должен подождать, когда обработается текущее событие
	}

	// добавляем нужное событие в epoll_ctl
	switch unit.EventType {

	// хотим узнать результат подключения к серверу
	case "connect":
		err := e.addConnectEvent(unit.SocketFd)
		if err != nil {
			return err
		}

	// хотим узнать факт получения входящего сообщения
	case "income":
		err := e.addIncomeEvent(unit.SocketFd)
		if err != nil {
			return err
		}

	// хотим узнать результат отправки своего сообщения
	case "outcome":
		err := e.addOutcomeEvent(unit.SocketFd)
		if err != nil {
			return err
		}

	default:
		return models.ErrPollUnknownEventType

	}

	e.addSocketInPolling(unit)

	// проверка, происходит ли поллинг. Если да - конец
	// Если нет - запускаем его
	if !e.isPolling() {
		e.startPolling()
		go e.wait()
	}

	return nil
}

// wait делает системный вызов epoll_wait с нулевым таймаутом
//
// Крутится, пока не получит события по всем ждущим сокетам
func (e *Epoll) wait() {
	for {
		n, err := e.sys.Wait(e.fd, e.eventsBuf, 0)
		if err != nil {
			e.setError(err)
			go e.pushError()

			break
		}
		if n > 0 { // Пришли какие-то события
			e.mu.Lock()
			e.addReadyEvents(e.eventsBuf[:n])
			go e.processEvents(n)

			if n == e.pollingSocketsLen() { // готовы все ожидаемые сокеты
				e.stopPolling()
				e.mu.Unlock()
				
				break
			}
			e.mu.Unlock()
		}
	}
}

// processEvents обрабатывает полученные события, находит готовые сокеты и возвращает результаты ждущим потокам
func (e *Epoll) processEvents(readySocketsLen int) {
	e.mu.Lock()

	defer e.mu.Unlock()

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
			if e.getSocketEventType(socketFd) == "income" { // если ждем именно это событие
				readySockets[socketFd] = models.PollingResult{}

				continue
			}
			readySockets[socketFd] = models.PollingResult{
				Err: models.ErrPollDiffEventType,
			}
		}
		if v.Events & syscall.EPOLLOUT != 0 { // буфер отправки пуст
			if e.getSocketEventType(socketFd) == "outcome" || e.getSocketEventType(socketFd) == "connect" { // если ждем именно это событие
				readySockets[socketFd] = models.PollingResult{}

				continue
			}
			readySockets[socketFd] = models.PollingResult{
				Err: models.ErrPollDiffEventType,
			}
		}
	}

	// если произошла некая рассинхронизация или странная ситуация. Такого не должно происходить
	if len(readySockets) != readySocketsLen {
		panic("not all expected sockets are ready")
	}

	// возвращаем результаты, ждущие потоки могут продолжить свое выполнение
	for s, v := range readySockets {
		if ch := e.getSocketResultChan(s); ch == nil {
			e.setSocketUnexpErr(s, v.Err) // случай, когда пришел результат, который никто не ждет
		} else {
			ch <- v.Err
		}
		e.deleteSocketFromPolling(s)
	}
	e.clearReadyEvents() // удаляем завершенные события
}

func (e *Epoll) DeleteSocketFromPolling(socketFd int) { // написать тесты
	e.mu.Lock()

	defer e.mu.Unlock()
	
	e.deleteSocketFromPolling(socketFd)
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

	for s := range e.sockets {
		e.getSocketResultChan(s) <- err
	}
}


// ------------------------------------------------
// Методы, которые должны вызываться только под захваченным мьютексом

func (e *Epoll) isSocketInPolling(socketFd int) bool {
	_, ok := e.sockets[socketFd]
	return ok
}

func (e *Epoll) addSocketInPolling(unit models.PollingUnit) {
	e.sockets[unit.SocketFd] = unit
}

func (e *Epoll) deleteSocketFromPolling(socketFd int) {
	delete(e.sockets, socketFd)
}

func (e *Epoll) isPolling() bool {
	return e.polling
}

func (e *Epoll) startPolling() {
	e.polling = true
}

func (e *Epoll) stopPolling() {
	e.polling = false
}

func (e *Epoll) pollingSocketsLen() int {
	return len(e.sockets)
}

func (e *Epoll) newOutcomeEvent(socketFd int) syscall.EpollEvent {
	return syscall.EpollEvent{
		Events: syscall.EPOLLOUT,
		Fd: int32(socketFd),
		Pad: 0, // узнать, что это
	}
}

func (e *Epoll) newIncomeEvent(socketFd int) syscall.EpollEvent {
	return syscall.EpollEvent{
		Events: syscall.EPOLLIN,
		Fd: int32(socketFd),
		Pad: 0, // узнать, что это
	}
}

func (e *Epoll) addConnectEvent(socketFd int) error {
	event := e.newOutcomeEvent(socketFd)
	
	err := e.sys.Ctl(e.fd, syscall.EPOLL_CTL_ADD, socketFd, &event)
	if err != nil {
		return e.handleEpollError(err)
	}
	
	return nil
}

func (e *Epoll) addIncomeEvent(socketFd int) error {
	event := e.newIncomeEvent(socketFd)

	err := e.sys.Ctl(e.fd, syscall.EPOLL_CTL_MOD, socketFd, &event)
	if err != nil {
		return e.handleEpollError(err)
	}
	
	return nil
}

func (e *Epoll) addOutcomeEvent(socketFd int) error {
	event := e.newOutcomeEvent(socketFd)

	err := e.sys.Ctl(e.fd, syscall.EPOLL_CTL_MOD, socketFd, &event)
	if err != nil {
		return e.handleEpollError(err)
	}
	
	return nil
}

// Подумать над удалением сокета из interest list через ctl.EPOLL_CTL_DEL

func (e *Epoll) getSocketResultChan(socketFd int) chan error {
	return e.sockets[socketFd].ResultChan
}

func (e *Epoll) getSocketEventType(socketFd int) string {
	return e.sockets[socketFd].EventType
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

func (e *Epoll) getSocketUnexpErr(socketFd int) error {
	return e.socketsUnexpErr[socketFd]
}

func (e *Epoll) deleteSocketUnexpErr(socketFd int) {
	delete(e.socketsUnexpErr, socketFd)
}

func (e *Epoll) handleEpollError(err error) error {
	// EBADF  epfd or fd is not a valid file descriptor.
	if errors.Is(err, syscall.EBADF) {
		return models.ErrPollBadFD
	}

	// EEXIST op was EPOLL_CTL_ADD, and the supplied file descriptor fd is already registered  with  this  epoll  in‐
	// 	stance.
	if errors.Is(err, syscall.EEXIST) {
		return models.ErrSocketAlreadyAdded
	}

	// EINVAL epfd  is  not an epoll file descriptor, or fd is the same as epfd, or the requested operation op is not
	// 	supported by this interface.
	// EINVAL An invalid event type was specified along with EPOLLEXCLUSIVE in events.
	// EINVAL op was EPOLL_CTL_MOD and events included EPOLLEXCLUSIVE.
	// EINVAL op was EPOLL_CTL_MOD and the EPOLLEXCLUSIVE flag has previously been applied to this epfd, fd pair.
	// EINVAL EPOLLEXCLUSIVE was specified in event and fd refers to an epoll instance.
	// ELOOP  fd refers to an epoll instance and this EPOLL_CTL_ADD operation would result  in  a  circular  loop  of
	// 	epoll instances monitoring one another or a nesting depth of epoll instances greater than 5.

	// ENOENT op was EPOLL_CTL_MOD or EPOLL_CTL_DEL, and fd is not registered with this epoll instance.
	if errors.Is(err, syscall.ENOENT) {
		return models.ErrSocketNotAdded
	}

	// ENOMEM There was insufficient memory to handle the requested op control operation.
	if errors.Is(err, syscall.ENOMEM) {
		return models.ErrNoMemory
	}

	// ENOSPC The  limit  imposed  by  /proc/sys/fs/epoll/max_user_watches  was  encountered while trying to register
	// 	(EPOLL_CTL_ADD) a new file descriptor on an epoll instance.  See epoll(7) for further details.
	// EPERM  The target file fd does not support epoll.  This error can occur if fd refers to, for example, a  regu‐
	// 	lar file or a directory.

	return fmt.Errorf("epoll_ctl err: %w", err)
}