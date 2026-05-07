package main

type mock struct{}

func (m mock) Add() error {
	return nil
}

func main() {
	// парсим конфиг
	conf := &Config{}

	// создаем Balancer
	balancer := NewBalancer(conf)

	m := mock{}

	// создаем Linker
	linker, err := NewLinker(*balancer, m)
	if err != nil {
		panic(err)
	}

	// ждем входящие соединения
	linker.Serve()
}