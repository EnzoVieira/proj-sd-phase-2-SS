# Build integrado dos 3 componentes da Fase 2.
#   make        -> compila SS (Erlang), SA e DHT (Java/Maven)
#   make ss|sa|dht  -> compila só um componente
#   make clean  -> limpa os 3
# Nota: sa/ e dht/ são código do colega (não editar); ver README.md.

.PHONY: all ss sa dht clean

all: ss sa dht

ss:
	$(MAKE) -C ss

sa:
	cd sa && mvn -q clean compile

dht:
	cd dht/App && mvn -q clean compile

clean:
	$(MAKE) -C ss clean
	cd sa && mvn -q clean
	cd dht/App && mvn -q clean
