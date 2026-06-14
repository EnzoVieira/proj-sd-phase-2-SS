# Makefile do SS
# ----------------
# Compila src/*.erl e test/*.erl para ebin/*.beam, e copia o ficheiro de
# aplicação OTP (src/ss.app.src -> ebin/ss.app).

ERLC        = erlc
EBIN_DIR    = ebin
ERLC_FLAGS  = -Wall

# Dependência vendorizada: chumak (ZeroMQ em Erlang puro)
CHUMAK_DIR  = deps/chumak
CHUMAK_EBIN = $(CHUMAK_DIR)/ebin

# Procurar .erl tanto em src/ como em test/
vpath %.erl src test

SOURCES = $(wildcard src/*.erl) $(wildcard test/*.erl)
BEAMS   = $(patsubst %.erl,$(EBIN_DIR)/%.beam,$(notdir $(SOURCES)))

APP_SRC = src/ss.app.src
APP     = $(EBIN_DIR)/ss.app

all: $(EBIN_DIR) chumak $(APP) $(BEAMS)

# Compila o chumak só uma vez (se ainda não houver o seu ebin).
# Os nossos módulos usam chumak; é preciso ter este ebin no code path em runtime
# (correr o erl com:  -pa ebin -pa deps/chumak/ebin).
chumak: $(CHUMAK_EBIN)/chumak.app

$(CHUMAK_EBIN)/chumak.app:
	mkdir -p $(CHUMAK_EBIN)
	$(ERLC) -I $(CHUMAK_DIR)/include -o $(CHUMAK_EBIN) $(CHUMAK_DIR)/src/*.erl
	cp $(CHUMAK_DIR)/src/chumak.app.src $(CHUMAK_EBIN)/chumak.app

$(EBIN_DIR):
	mkdir -p $(EBIN_DIR)

# Copiar o ficheiro de aplicação para ebin/ (o OTP procura-o no code path)
$(APP): $(APP_SRC)
	cp $(APP_SRC) $(APP)

$(EBIN_DIR)/%.beam: %.erl
	$(ERLC) $(ERLC_FLAGS) -o $(EBIN_DIR) $<

# clean NÃO apaga o chumak (é uma dependência); usa clean-deps para isso.
clean:
	rm -rf $(EBIN_DIR)/*.beam $(APP)

clean-deps:
	rm -rf $(CHUMAK_EBIN)

.PHONY: all clean clean-deps chumak
