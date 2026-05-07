package main

func main() {
	// парсим конфиг
	conf := &Config{}

	// создаем Balancer
	balancer := NewBalancer(conf)

	// Balancer.Start()
	balancer.Start()
}