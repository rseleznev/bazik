package main

import (
	"github.com/rseleznev/bazik/config"
	"github.com/rseleznev/bazik/internal/balancer"
	"github.com/rseleznev/bazik/internal/linker"
)

type mock struct{}

func (m mock) Add() error {
	return nil
}
func (m mock) Close()

func main() {
	// парсим конфиг
	conf := &config.Config{}

	// создаем Router
	balancer := balancer.NewBalancer(conf)

	// временный мок поллера
	m := mock{}

	// создаем Linker
	linker, err := linker.NewLinker(conf, balancer, m)
	if err != nil {
		panic(err)
	}

	// слушаем входящие соединения
	linker.Serve()
}