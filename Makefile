# Makefile do SS
# ----------------
# Compila src/*.erl e test/*.erl para ebin/*.beam, e copia o ficheiro de
# aplicação OTP (src/ss.app.src -> ebin/ss.app).

ERLC        = erlc
EBIN_DIR    = ebin
ERLC_FLAGS  = -Wall

# Procurar .erl tanto em src/ como em test/
vpath %.erl src test

SOURCES = $(wildcard src/*.erl) $(wildcard test/*.erl)
# notdir tira a pasta: src/ss_tcp.erl -> ss_tcp.erl -> ebin/ss_tcp.beam
BEAMS   = $(patsubst %.erl,$(EBIN_DIR)/%.beam,$(notdir $(SOURCES)))

APP_SRC = src/ss.app.src
APP     = $(EBIN_DIR)/ss.app

all: $(EBIN_DIR) $(APP) $(BEAMS)

$(EBIN_DIR):
	mkdir -p $(EBIN_DIR)

# Copiar o ficheiro de aplicação para ebin/ (o OTP procura-o no code path)
$(APP): $(APP_SRC)
	cp $(APP_SRC) $(APP)

$(EBIN_DIR)/%.beam: %.erl
	$(ERLC) $(ERLC_FLAGS) -o $(EBIN_DIR) $<

clean:
	rm -rf $(EBIN_DIR)/*.beam $(APP)

.PHONY: all clean
