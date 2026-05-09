package main

import (
	"github.com/rseleznev/bazik/config"
	"github.com/rseleznev/bazik/internal/balancer"
	"github.com/rseleznev/bazik/internal/handler"
)

func main() {
	// создаем controller
	ctl := controller{}
	
	// парсим stdin (флаги, ссылку на конфиг)
	
	// парсим конфиг
	conf := ctl.parseConfig(ctl.flags[0])

	// запускаем
	ctl.run(conf)
}

type controller struct {
	flags []string
	
	blncr balancer.Balancer
}

func (c *controller) parseConfig(_ string) *config.Config {
	conf := config.Config{
		BalancingAlg: "random",
	}
	
	return &conf
}

func (c *controller) run(conf *config.Config) {
	// создаем Handler
	h := handler.NewHandler(conf)
	
	// создаем Balancer
	c.blncr = balancer.NewBalancer(conf, h)

	// слушаем входящие соединения
	c.blncr.Start()
}