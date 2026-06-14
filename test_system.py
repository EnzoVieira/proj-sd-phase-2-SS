#!/usr/bin/env python3
"""
test_system.py — Testes de escalabilidade e robustez do sistema SD completo.

Arranca automaticamente:
  2x DHT/AST  (portas 7878, 7879)
  2x SA       (portas 9090, 9091)
  2x SS       (portas 5555, 5556)

Fases de teste:
  1. Smoke          — autenticação, credenciais erradas, sem autenticar
  2. Estado         — online_count, is_online, active_count, por tipo
  3. Agregação      — COUNT/SUM/MAX/MIN/SUM_PRODUCT via SS→SA→DHT
  4. Produtores     — simulação de N produtores concorrentes (throughput + latência)
  5. Consumidores   — N queries concorrentes até 20 threads
  6. Saturação      — escala até 150 consumidores para encontrar ponto de rutura
  7. Robustez       — clientes lentos, desligamentos abruptos

Saída:
  results_<timestamp>.png  — gráficos throughput + latência
  results_<timestamp>.csv  — dados brutos

Requisitos: Python 3.10+, Java 17+, Maven 3.6+, Erlang/OTP 26+, GNU make
  Gráficos: pip install matplotlib
"""

import asyncio
import collections
import concurrent.futures
import csv
import dataclasses
import datetime
import json
import math
import os
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import List, Optional, Tuple

# ══════════════════════════════════════════════════════════════════════
# Configuração
# ══════════════════════════════════════════════════════════════════════

ROOT    = Path(__file__).resolve().parent
SS_DIR  = ROOT / "ss"
DHT_POM = ROOT / "dht" / "App" / "pom.xml"
SA_POM  = ROOT / "sa"  / "pom.xml"

DHT1_PORT = 7878
DHT2_PORT = 7879
SA_N_PORT = 9090
SA_S_PORT = 9091
SS_N_PORT = 5555
SS_S_PORT = 5556
GOSSIP_N  = 6000
GOSSIP_S  = 6001

INDEX_FIELDS = "zone,type"

# Seed: intervalo mais largo → mais eventos para agregação
SEED_START = "2026-01-01"
SEED_END   = "2026-06-09"   # estritamente antes de hoje (2026-06-14)

STARTUP_TIMEOUT = 60    # s — aguardar porto TCP
GOSSIP_SETTLE   = 2     # s — anti-entropia entre nós SS
TEST_TIMEOUT    = 15    # s — timeout por operação de teste

# Produtores (fase 4)
PROD_DURATION = 10          # s por nível de concorrência
PROD_LEVELS   = [1, 2, 3, 5]   # 5 dispositivos registados no máximo

# Consumidores (fase 5)
CONS_DURATION = 8
CONS_LEVELS   = [1, 2, 5, 10, 20]

# Saturação (fase 6)
SAT_DURATION    = 8         # s por nível
SAT_LEVELS      = [1, 5, 10, 20, 40, 80, 150]
SAT_P99_LIMIT   = 10_000    # ms — se p99 ultrapassar este valor, marcar saturado e parar

# Credenciais — devem corresponder a ss/priv/devices.json e ss/priv/consumers.json
DEVICES = [
    ("car-001",   "pass1", "car"),
    ("car-002",   "pass2", "car"),
    ("drone-001", "pass3", "drone"),
    ("drone-002", "pass4", "drone"),
    ("truck-001", "pass5", "truck"),
]
CONSUMERS = [
    ("alice", "alice123"),
    ("bob",   "bob123"),
]

# ══════════════════════════════════════════════════════════════════════
# Output formatado
# ══════════════════════════════════════════════════════════════════════

def _c(code, t): return f"\033[{code}m{t}\033[0m"
def h1(t):   print(f"\n{_c('1','='*68)}\n{_c('1', t)}")
def h2(t):   print(f"\n{_c('1', t)}")
def ok(t):   print(f"  {_c('32','[OK]  ')} {t}")
def fail(t): print(f"  {_c('31','[FAIL]')} {t}")
def warn(t): print(f"  {_c('33','[WARN]')} {t}")
def log(t):  print(f"  {_c('36','[LOG] ')} {t}")

# ══════════════════════════════════════════════════════════════════════
# Gestão de processos
# ══════════════════════════════════════════════════════════════════════

_procs: List[subprocess.Popen] = []
_plock = threading.Lock()

def _register(p: subprocess.Popen) -> subprocess.Popen:
    with _plock: _procs.append(p)
    return p

def _drain(p: subprocess.Popen, label: str):
    def _w():
        try:
            for _ in p.stdout: pass
        except: pass
    threading.Thread(target=_w, daemon=True, name=f"drain-{label}").start()

def _kill_all():
    with _plock:
        for p in _procs:
            try: p.kill()
            except: pass

signal.signal(signal.SIGINT,  lambda *_: (_kill_all(), sys.exit(1)))
signal.signal(signal.SIGTERM, lambda *_: (_kill_all(), sys.exit(1)))

def wait_port(port: int, host: str = "localhost",
              timeout: float = STARTUP_TIMEOUT) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1): return True
        except OSError: time.sleep(0.5)
    return False

def _find(name: str) -> str:
    for n in [name, name + ".cmd", name + ".bat"]:
        found = shutil.which(n)
        if found:
            return found   # caminho absoluto — necessário no Windows para .cmd/.bat
    return name

MVN  = _find("mvn")
ERL  = _find("erl")
ERLC = _find("erlc")
MAKE = _find("make")

# ══════════════════════════════════════════════════════════════════════
# Build
# ══════════════════════════════════════════════════════════════════════

def build_java(pom: Path, label: str) -> bool:
    log(f"A compilar {label}...")
    r = subprocess.run(
        [MVN, "-f", str(pom), "package", "-q", "-DskipTests"],
        capture_output=True, text=True)
    if r.returncode != 0:
        fail(f"Falha ao compilar {label}:\n{r.stderr[-500:]}")
        return False
    ok(f"{label} compilado")
    return True

def build_erlang() -> bool:
    """Compila SS + chumak invocando erlc diretamente (sem make, funciona em Windows)."""
    log("A compilar SS (Erlang + chumak)...")

    chumak_dir  = SS_DIR / "deps" / "chumak"
    chumak_ebin = chumak_dir / "ebin"
    chumak_src  = chumak_dir / "src"
    chumak_inc  = chumak_dir / "include"
    ebin_dir    = SS_DIR / "ebin"
    src_dir     = SS_DIR / "src"
    test_dir    = SS_DIR / "test"

    chumak_ebin.mkdir(parents=True, exist_ok=True)
    ebin_dir.mkdir(parents=True, exist_ok=True)

    # Compila chumak apenas se ainda não estiver compilado
    if not (chumak_ebin / "chumak.app").exists():
        erl_files = sorted(chumak_src.glob("*.erl"))
        if not erl_files:
            fail("Fontes do chumak não encontradas em deps/chumak/src/")
            return False
        r = subprocess.run(
            [ERLC, "-I", str(chumak_inc), "-o", str(chumak_ebin)]
            + [str(f) for f in erl_files],
            capture_output=True, text=True, cwd=SS_DIR)
        if r.returncode != 0:
            fail(f"Falha ao compilar chumak:\n{r.stderr[-500:]}")
            return False
        app_src = chumak_src / "chumak.app.src"
        if app_src.exists():
            shutil.copy2(app_src, chumak_ebin / "chumak.app")

    # Copia o ficheiro de aplicação OTP
    app_src = src_dir / "ss.app.src"
    if app_src.exists():
        shutil.copy2(app_src, ebin_dir / "ss.app")

    # Compila módulos SS (src/ + test/ se existir)
    erl_files = sorted(src_dir.glob("*.erl"))
    if test_dir.exists():
        erl_files += sorted(test_dir.glob("*.erl"))
    if not erl_files:
        fail("Nenhum ficheiro .erl encontrado em ss/src/")
        return False

    r = subprocess.run(
        [ERLC, "-Wall", "-pa", str(chumak_ebin),
         "-o", str(ebin_dir)] + [str(f) for f in erl_files],
        capture_output=True, text=True, cwd=SS_DIR)
    if r.returncode != 0:
        fail(f"Falha ao compilar SS:\n{r.stderr[-500:]}")
        return False

    ok("SS compilado")
    return True

# ══════════════════════════════════════════════════════════════════════
# Launch
# ══════════════════════════════════════════════════════════════════════

def launch_java(pom: Path, main_class: str, args: str, label: str) -> subprocess.Popen:
    cmd = [MVN, "-f", str(pom), "exec:java", "-q",
           f"-Dexec.mainClass={main_class}",
           f"-Dexec.args={args}"]
    p = _register(subprocess.Popen(
        cmd, cwd=ROOT,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True))
    _drain(p, label)
    return p

def _gen_ss_config(zone: str, port: int, gport: int, gpeer: int, sa_port: int) -> Path:
    cfg = SS_DIR / f"node_{zone}.config"
    cfg.write_text(
        f"[{{ss, [\n"
        f"    {{port,            {port}}},\n"
        f"    {{zone,            {zone}}},\n"
        f"    {{gossip_port,     {gport}}},\n"
        f"    {{gossip_peers,    [{{\"localhost\", {gpeer}}}]}},\n"
        f"    {{gossip_interval, 400}},\n"
        f"    {{dht_host,        \"localhost\"}},\n"
        f"    {{dht_port,        {DHT1_PORT}}},\n"
        f"    {{dht_index_fields, [zone, type]}},\n"
        f"    {{sa_host,         \"localhost\"}},\n"
        f"    {{sa_port,         {sa_port}}}\n"
        f"]}}].\n",
        encoding="utf-8")
    return cfg

def launch_ss(zone: str, port: int, gport: int, gpeer: int, sa_port: int) -> subprocess.Popen:
    cfg = _gen_ss_config(zone, port, gport, gpeer, sa_port)
    cmd = [ERL,
           "-pa", "ebin",
           "-pa", "deps/chumak/ebin",
           "-config", str(cfg.with_suffix("")),
           "-eval", "application:start(ss)",
           "-noshell"]
    p = _register(subprocess.Popen(
        cmd, cwd=SS_DIR,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True))
    _drain(p, f"ss-{zone}")
    return p

# ══════════════════════════════════════════════════════════════════════
# Protocolo SS (TCP / JSON line-delimited)
# ══════════════════════════════════════════════════════════════════════

class SSClient:
    def __init__(self, host: str = "localhost", port: int = SS_N_PORT,
                 timeout: float = TEST_TIMEOUT):
        self._s = socket.create_connection((host, port), timeout=timeout)
        self._f = self._s.makefile("r", encoding="utf-8")

    def send(self, obj: dict) -> dict:
        self._s.sendall((json.dumps(obj) + "\n").encode())
        line = self._f.readline()
        if not line:
            raise ConnectionError("SS fechou a ligação inesperadamente")
        return json.loads(line)

    def close(self):
        try: self._s.close()
        except: pass

    def __enter__(self): return self
    def __exit__(self, *_): self.close()

def auth_prod(c: SSClient, dev: str, pw: str) -> bool:
    return c.send({"cmd": "auth_producer", "device": dev, "password": pw}).get("status") == "ok"

def auth_cons(c: SSClient, user: str, pw: str) -> bool:
    return c.send({"cmd": "auth_consumer", "user": user, "password": pw}).get("status") == "ok"

def send_ev(c: SSClient, etype: str, extra: dict = None) -> bool:
    p = {"cmd": "event", "type": etype, "timestamp": int(time.time() * 1000)}
    if extra: p.update(extra)
    return c.send(p).get("status") == "ok"

# ══════════════════════════════════════════════════════════════════════
# Métricas
# ══════════════════════════════════════════════════════════════════════

@dataclasses.dataclass
class RunMetrics:
    phase:        str
    label:        str
    n:            int      # concorrência
    throughput:   float    # ops/s
    lat_p50:      float    # ms
    lat_p95:      float    # ms
    lat_p99:      float    # ms
    lat_max:      float    # ms
    errors:       int
    total:        int

    @property
    def success_rate(self) -> float:
        return self.total / (self.total + self.errors) if (self.total + self.errors) > 0 else 0.0

def _pct(s: list, p: int) -> float:
    if not s: return 0.0
    idx = min(int(p / 100 * len(s)), len(s) - 1)
    return s[idx]

def _build_metrics(phase: str, label: str, n: int,
                   lats: list, errors: int, elapsed: float) -> RunMetrics:
    s = sorted(lats)
    return RunMetrics(
        phase=phase, label=label, n=n,
        throughput=len(s) / elapsed if elapsed > 0 else 0.0,
        lat_p50=_pct(s, 50), lat_p95=_pct(s, 95),
        lat_p99=_pct(s, 99), lat_max=max(s) if s else 0.0,
        errors=errors, total=len(s))

_prod_metrics: List[RunMetrics] = []
_cons_metrics: List[RunMetrics] = []
_sat_metrics:  List[RunMetrics] = []

# ══════════════════════════════════════════════════════════════════════
# Resultados de testes (pass/fail)
# ══════════════════════════════════════════════════════════════════════

_results: List[Tuple[str, bool, str]] = []

def check(name: str, passed: bool, detail: str = "") -> bool:
    _results.append((name, passed, detail))
    (ok if passed else fail)(f"{name:<56} {detail}")
    return passed

# ══════════════════════════════════════════════════════════════════════
# 1. Smoke
# ══════════════════════════════════════════════════════════════════════

def test_smoke():
    h2("1. Smoke — autenticação e envio básico")
    dev, pw, _ = DEVICES[0]
    user, upw  = CONSUMERS[0]

    try:
        with SSClient(port=SS_N_PORT) as c:
            check("auth_producer_ok", auth_prod(c, dev, pw))
            check("event_send_ok",    send_ev(c, "alarm", {"temperature": "25.5", "zone": "north"}))
    except Exception as e:
        check("smoke_producer", False, str(e))

    try:
        with SSClient(port=SS_N_PORT) as c:
            check("auth_consumer_ok", auth_cons(c, user, upw))
    except Exception as e:
        check("smoke_consumer", False, str(e))

    try:
        with SSClient(port=SS_N_PORT) as c:
            r = c.send({"cmd": "auth_producer", "device": dev, "password": "ERRADA"})
            check("auth_wrong_password", r.get("code") == 401, str(r))
    except Exception as e:
        check("auth_wrong_password", False, str(e))

    try:
        with SSClient(port=SS_N_PORT) as c:
            r = c.send({"cmd": "online_count"})
            check("cmd_without_auth_401", r.get("code") == 401, str(r))
    except Exception as e:
        check("cmd_without_auth_401", False, str(e))

    try:
        with SSClient(port=SS_N_PORT) as c:
            auth_prod(c, dev, pw)
            r = c.send({"cmd": "online_count"})
            check("producer_cant_query", r.get("code") == 401, str(r))
    except Exception as e:
        check("producer_cant_query", False, str(e))

# ══════════════════════════════════════════════════════════════════════
# 2. Estado global
# ══════════════════════════════════════════════════════════════════════

def test_state():
    h2("2. Estado global — online, activo, contagem por tipo")
    user, pw = CONSUMERS[0]

    conns = []
    for d, dpw, _ in DEVICES[:3]:
        try:
            c = SSClient(port=SS_N_PORT, timeout=5)
            auth_prod(c, d, dpw)
            send_ev(c, "heartbeat", {"zone": "north"})
            conns.append(c)
        except: pass

    time.sleep(GOSSIP_SETTLE)

    try:
        with SSClient(port=SS_N_PORT) as c:
            auth_cons(c, user, pw)
            r = c.send({"cmd": "online_count"})
            check("online_count >= 1",
                  r.get("result", {}).get("online", 0) >= 1, str(r.get("result")))
            r = c.send({"cmd": "is_online", "device": DEVICES[0][0]})
            check("is_online(car-001) = true",
                  r.get("result", {}).get("online") is True, str(r.get("result")))
            r = c.send({"cmd": "active_count"})
            check("active_count >= 1",
                  r.get("result", {}).get("active", 0) >= 1, str(r.get("result")))
            r = c.send({"cmd": "online_count_by_type", "type": "car"})
            check("online_car >= 1",
                  r.get("result", {}).get("online", 0) >= 1, str(r.get("result")))
            r = c.send({"cmd": "online_count_by_type", "type": "submarine"})
            check("online_submarine = 0",
                  r.get("status") == "ok" and r.get("result", {}).get("online", 0) == 0,
                  str(r.get("result")))
    except Exception as e:
        check("state_queries", False, str(e))
    finally:
        for c in conns: c.close()

    time.sleep(GOSSIP_SETTLE)
    try:
        with SSClient(port=SS_S_PORT) as c:
            auth_cons(c, user, pw)
            r = c.send({"cmd": "online_count"})
            check("gossip_south_sees_online",
                  r.get("result", {}).get("online", 0) >= 1, str(r.get("result")))
    except Exception as e:
        check("gossip_south_sees_online", False, str(e))

# ══════════════════════════════════════════════════════════════════════
# 3. Agregação via SS→SA→DHT
# ══════════════════════════════════════════════════════════════════════

def test_aggregations():
    h2("3. Agregação — COUNT/SUM/MAX/MIN/SUM_PRODUCT via SS→SA→DHT")
    user, pw = CONSUMERS[0]

    cases = [
        ("COUNT",       None,          None),
        ("SUM",         "temperature", None),
        ("MAX",         "temperature", None),
        ("MIN",         "temperature", None),
        ("SUM_PRODUCT", "temperature", "pressure"),
    ]

    try:
        with SSClient(port=SS_N_PORT, timeout=30) as c:
            auth_cons(c, user, pw)
            for atype, k2, k3 in cases:
                req = {"cmd": "aggregate", "type": atype,
                       "minDay": SEED_START, "maxDay": SEED_END,
                       "indexField": "zone", "indexValue": "north"}
                if k2: req["k2"] = k2
                if k3: req["k3"] = k3
                t0  = time.time()
                r   = c.send(req)
                lat = (time.time() - t0) * 1000
                res = r.get("result", {})
                check(f"aggregate_{atype}",
                      r.get("status") == "ok",
                      f"count={res.get('count','?')}  lat={lat:.0f}ms")
    except Exception as e:
        check("aggregations", False, str(e))

    # Cache hit
    try:
        with SSClient(port=SS_N_PORT, timeout=15) as c:
            auth_cons(c, user, pw)
            req = {"cmd": "aggregate", "type": "COUNT",
                   "minDay": SEED_START, "maxDay": SEED_END,
                   "indexField": "zone", "indexValue": "north"}
            t0 = time.time()
            r  = c.send(req)
            lat_cache = (time.time() - t0) * 1000
            check("aggregate_cache_hit", r.get("status") == "ok",
                  f"lat_cache={lat_cache:.0f}ms")
    except Exception as e:
        check("aggregate_cache_hit", False, str(e))

    # Rejeitar dia corrente
    try:
        today = datetime.date.today().isoformat()
        with SSClient(port=SS_N_PORT) as c:
            auth_cons(c, user, pw)
            r = c.send({"cmd": "aggregate", "type": "COUNT",
                        "minDay": today, "maxDay": today,
                        "indexField": "zone", "indexValue": "north"})
            check("aggregate_rejects_today", r.get("status") != "ok", str(r))
    except Exception as e:
        check("aggregate_rejects_today", False, str(e))

    # Cache distribuída via peer SA
    try:
        with SSClient(port=SS_S_PORT, timeout=15) as c:
            auth_cons(c, user, pw)
            req = {"cmd": "aggregate", "type": "COUNT",
                   "minDay": SEED_START, "maxDay": SEED_END,
                   "indexField": "zone", "indexValue": "north"}
            t0 = time.time()
            r  = c.send(req)
            lat = (time.time() - t0) * 1000
            check("aggregate_peer_cache", r.get("status") == "ok",
                  f"lat={lat:.0f}ms (SA-south→peer→SA-north)")
    except Exception as e:
        check("aggregate_peer_cache", False, str(e))

# ══════════════════════════════════════════════════════════════════════
# Workers reutilizáveis para fases de carga
# ══════════════════════════════════════════════════════════════════════

def _producer_worker(dev: str, pw: str, port: int,
                     duration_s: float, lats: list, errs: list, stop: threading.Event):
    """Envia eventos em loop durante duration_s segundos. Thread-safe para append."""
    deadline = time.time() + duration_s
    ev_idx   = 0
    try:
        with SSClient(port=port, timeout=duration_s + 10) as c:
            if not auth_prod(c, dev, pw):
                errs.append(1); return
            while time.time() < deadline and not stop.is_set():
                ev_idx += 1
                temp = f"{18.0 + (ev_idx % 20):.1f}"
                t0 = time.time()
                try:
                    if send_ev(c, "telemetry", {"zone": "north", "temperature": temp,
                                                 "pressure": str(100 + ev_idx % 10)}):
                        lats.append((time.time() - t0) * 1000)
                    else:
                        errs.append(1)
                except Exception:
                    errs.append(1)
    except Exception:
        errs.append(1)

_AGG_REQ = {
    "cmd": "aggregate", "type": "COUNT",
    "minDay": SEED_START, "maxDay": SEED_END,
    "indexField": "zone", "indexValue": "north",
}

def _consumer_worker(user: str, pw: str, port: int,
                     duration_s: float, lats: list, errs: list, stop: threading.Event):
    """Envia queries em loop durante duration_s segundos."""
    deadline = time.time() + duration_s
    try:
        with SSClient(port=port, timeout=duration_s + 15) as c:
            if not auth_cons(c, user, pw):
                errs.append(1); return
            while time.time() < deadline and not stop.is_set():
                t0 = time.time()
                try:
                    r = c.send(_AGG_REQ)
                    lat = (time.time() - t0) * 1000
                    if r.get("status") == "ok":
                        lats.append(lat)
                    else:
                        errs.append(1)
                except Exception:
                    errs.append(1)
    except Exception:
        errs.append(1)

def _run_load(phase: str, label: str, n: int,
              worker_fn, worker_args_list: list,
              duration_s: float) -> RunMetrics:
    """Executa n workers concorrentes durante duration_s e devolve métricas."""
    all_lats: list = []
    all_errs: list = []
    stop = threading.Event()

    lats_per = [[] for _ in range(n)]
    errs_per = [[] for _ in range(n)]

    t0 = time.time()
    threads = []
    for i in range(n):
        args = worker_args_list[i] + [duration_s, lats_per[i], errs_per[i], stop]
        t = threading.Thread(target=worker_fn, args=args, daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=duration_s + 20)

    elapsed = time.time() - t0
    for lp, ep in zip(lats_per, errs_per):
        all_lats.extend(lp)
        all_errs.extend(ep)

    return _build_metrics(phase, label, n, all_lats, len(all_errs), elapsed)

# ══════════════════════════════════════════════════════════════════════
# 4. Fase produtores — simulação real-time
# ══════════════════════════════════════════════════════════════════════

def test_producers():
    h2(f"4. Produtores — {PROD_LEVELS} concorrentes × {PROD_DURATION}s cada")

    for n in PROD_LEVELS:
        devs = [DEVICES[i % len(DEVICES)] for i in range(n)]
        args_list = [[d, pw, SS_N_PORT] for d, pw, _ in devs]
        m = _run_load("producers", f"prod_n{n}", n,
                      _producer_worker, args_list, PROD_DURATION)
        _prod_metrics.append(m)

        check(f"producers_n{n:>3}  tput={m.throughput:6.1f}ev/s"
              f"  p50={m.lat_p50:5.0f}ms  p95={m.lat_p95:5.0f}ms",
              m.success_rate >= 0.7,
              f"total={m.total}  erros={m.errors}")

# ══════════════════════════════════════════════════════════════════════
# 5. Fase consumidores — queries concorrentes
# ══════════════════════════════════════════════════════════════════════

def test_consumers():
    h2(f"5. Consumidores — {CONS_LEVELS} concorrentes × {CONS_DURATION}s cada")

    # Aquece a cache do SA com uma query inicial
    try:
        with SSClient(port=SS_N_PORT, timeout=30) as c:
            auth_cons(c, *CONSUMERS[0])
            c.send(_AGG_REQ)
    except Exception: pass

    for n in CONS_LEVELS:
        creds = [CONSUMERS[i % len(CONSUMERS)] for i in range(n)]
        args_list = [[u, pw, SS_N_PORT] for u, pw in creds]
        m = _run_load("consumers", f"cons_n{n}", n,
                      _consumer_worker, args_list, CONS_DURATION)
        _cons_metrics.append(m)

        check(f"consumers_n{n:>3}  tput={m.throughput:6.1f}q/s"
              f"  p50={m.lat_p50:5.0f}ms  p95={m.lat_p95:5.0f}ms",
              m.success_rate >= 0.7,
              f"total={m.total}  erros={m.errors}")

# ══════════════════════════════════════════════════════════════════════
# 6. Saturação — asyncio evita bottleneck de threads Python
#
# Usar 150 threads bloqueadas em recv() ia saturar o scheduler Python
# antes do sistema. Com asyncio um único event loop gere N coroutines
# via sockets não-bloqueantes — overhead negligenciável até 500+ conns.
# ══════════════════════════════════════════════════════════════════════

async def _aio_send(reader: asyncio.StreamReader,
                    writer: asyncio.StreamWriter,
                    obj: dict, timeout: float = 30.0) -> dict:
    writer.write((json.dumps(obj) + "\n").encode())
    await writer.drain()
    line = await asyncio.wait_for(reader.readline(), timeout=timeout)
    if not line:
        raise ConnectionError("EOF")
    return json.loads(line)


async def _aio_consumer_coro(user: str, pw: str, port: int,
                             deadline: float,
                             connect_sem: asyncio.Semaphore,
                             lats: list, errs: list) -> None:
    """Uma coroutine = um consumidor virtual. Sem threads extras."""
    try:
        async with connect_sem:   # limita handshakes simultâneos
            r, w = await asyncio.wait_for(
                asyncio.open_connection("localhost", port), timeout=10.0)
        try:
            resp = await _aio_send(r, w,
                       {"cmd": "auth_consumer", "user": user, "password": pw})
            if resp.get("status") != "ok":
                errs.append(1)
                return
            while time.time() < deadline:
                t0   = time.time()
                resp = await _aio_send(r, w, _AGG_REQ, timeout=60.0)
                if resp.get("status") == "ok":
                    lats.append((time.time() - t0) * 1000)
                else:
                    errs.append(1)
        finally:
            w.close()
            try: await asyncio.wait_for(w.wait_closed(), timeout=2.0)
            except Exception: pass
    except Exception:
        errs.append(1)


async def _aio_sat_run(n: int, duration_s: float) -> tuple:
    lats: list = []
    errs: list = []
    deadline   = time.time() + duration_s
    sem        = asyncio.Semaphore(30)   # no máximo 30 handshakes em simultâneo

    creds = [CONSUMERS[i % len(CONSUMERS)] for i in range(n)]
    t0 = time.time()
    await asyncio.gather(
        *[_aio_consumer_coro(u, pw,
                             SS_N_PORT if i % 2 == 0 else SS_S_PORT,
                             deadline, sem, lats, errs)
          for i, (u, pw) in enumerate(creds)],
        return_exceptions=True)
    return lats, errs, time.time() - t0


def test_saturation():
    h2(f"6. Saturação (asyncio) — ramp até {SAT_LEVELS[-1]} consumidores"
       f"  (p99 limit={SAT_P99_LIMIT}ms)")

    # Aquece a cache SA para que a latência medida seja a de query, não de DHT
    try:
        with SSClient(port=SS_N_PORT, timeout=30) as c:
            auth_cons(c, *CONSUMERS[0])
            c.send(_AGG_REQ)
    except Exception: pass

    saturated = False
    for n in SAT_LEVELS:
        lats, errs, elapsed = asyncio.run(_aio_sat_run(n, SAT_DURATION))
        m = _build_metrics("saturation", f"sat_n{n}", n, lats, len(errs), elapsed)
        _sat_metrics.append(m)

        sat_flag = "  *** SATURADO ***" if m.lat_p99 > SAT_P99_LIMIT else ""
        check(f"saturation_n{n:>4}  tput={m.throughput:6.1f}q/s"
              f"  p50={m.lat_p50:5.0f}ms  p99={m.lat_p99:6.0f}ms{sat_flag}",
              m.success_rate >= 0.5,
              f"total={m.total}  erros={m.errors}")

        if m.lat_p99 > SAT_P99_LIMIT and not saturated:
            saturated = True
            warn(f"Sistema saturado com {n} consumidores"
                 f" (p99={m.lat_p99:.0f}ms > {SAT_P99_LIMIT}ms). A parar ramp.")
            break

    if not saturated:
        ok(f"Sistema NÃO saturou até {SAT_LEVELS[-1]} consumidores"
           f" (p99 max={max(m.lat_p99 for m in _sat_metrics):.0f}ms)")

# ══════════════════════════════════════════════════════════════════════
# 7. Robustez
# ══════════════════════════════════════════════════════════════════════

def _sa_agg_bytes() -> bytes:
    return (json.dumps({
        "type": "COUNT", "zone": "north",
        "minDay": SEED_START, "maxDay": SEED_END,
        "indexField": "zone", "indexValue": "north",
        "k2": None, "k3": None,
    }) + "\n").encode()

def test_robustness():
    h2("7. Robustez — clientes lentos e desligamentos abruptos")

    # 7a. N clientes SA que enviam pedido mas nunca lêem a resposta
    N_STUCK = 8
    stuck = []
    for _ in range(N_STUCK):
        try:
            s = socket.create_connection(("localhost", SA_N_PORT), timeout=5)
            s.sendall(_sa_agg_bytes())
            stuck.append(s)
        except Exception as e:
            warn(f"stuck socket: {e}")

    log(f"{len(stuck)} clientes stuck no SA-north (sem ler resposta)")
    time.sleep(1.5)

    try:
        with socket.create_connection(("localhost", SA_N_PORT), timeout=12) as s:
            s.sendall(_sa_agg_bytes())
            s.settimeout(12)
            r = json.loads(s.makefile("r").readline())
            check("sa_responsive_with_stuck_clients", "count" in r,
                  f"count={r.get('count','?')}")
    except Exception as e:
        check("sa_responsive_with_stuck_clients", False, str(e))
    finally:
        for s in stuck:
            try: s.close()
            except: pass

    time.sleep(0.5)

    # 7b. SA ainda funciona depois
    try:
        with socket.create_connection(("localhost", SA_N_PORT), timeout=5) as s:
            s.sendall(_sa_agg_bytes())
            s.settimeout(10)
            r = json.loads(s.makefile("r").readline())
            check("sa_alive_after_stuck_close", "count" in r,
                  f"count={r.get('count','?')}")
    except Exception as e:
        check("sa_alive_after_stuck_close", False, str(e))

    # 7c. Dispositivo que se desliga abruptamente e volta a ligar
    dev, dpw, _ = DEVICES[1]
    try:
        c = SSClient(port=SS_N_PORT, timeout=5)
        auth_prod(c, dev, dpw)
        send_ev(c, "alarm", {"zone": "north"})
        c._s.close()
    except: pass

    time.sleep(0.5)
    try:
        with SSClient(port=SS_N_PORT, timeout=5) as c:
            check("ss_reconnect_after_crash", auth_prod(c, dev, dpw))
    except Exception as e:
        check("ss_reconnect_after_crash", False, str(e))

    # 7d. Comando desconhecido não derruba o SS
    try:
        with SSClient(port=SS_N_PORT, timeout=5) as c:
            r = c.send({"cmd": "NONEXISTENT_CMD_XYZ"})
            check("ss_handles_unknown_cmd", "code" in r or "error" in r, str(r))
    except Exception as e:
        warn(f"ss_handles_unknown_cmd: ligação fechada ({e}) — a verificar SS activo...")
        try:
            with SSClient(port=SS_N_PORT, timeout=5) as c2:
                auth_cons(c2, *CONSUMERS[0])
                r2 = c2.send({"cmd": "online_count"})
                check("ss_handles_unknown_cmd", r2.get("status") == "ok",
                      "SS recuperou e responde normalmente")
        except Exception as e2:
            check("ss_handles_unknown_cmd", False, str(e2))

    # 7e. SS-south independente
    try:
        with SSClient(port=SS_S_PORT, timeout=5) as c:
            r = c.send({"cmd": "online_count"})
            check("ss_south_independent", r.get("code") == 401, str(r))
    except Exception as e:
        check("ss_south_independent", False, str(e))

# ══════════════════════════════════════════════════════════════════════
# Seed — dados históricos via FakeSSMain (intervalo largo)
# ══════════════════════════════════════════════════════════════════════

def seed_data() -> bool:
    log(f"A inserir dados históricos via FakeSSMain ({SEED_START} → {SEED_END})...")
    r = subprocess.run(
        [MVN, "-f", str(DHT_POM), "-q", "exec:java",
         "-Dexec.mainClass=pt.ua.FakeSSMain",
         f"-Dexec.args=localhost {DHT1_PORT} {INDEX_FIELDS} {SEED_START} {SEED_END}"],
        capture_output=True, text=True, timeout=300)
    if r.returncode != 0:
        fail(f"FakeSSMain falhou:\n{r.stdout[-400:]}\n{r.stderr[-200:]}")
        return False
    ok(f"Dados históricos inseridos no DHT (zone + type, {SEED_START} → {SEED_END})")
    return True

# ══════════════════════════════════════════════════════════════════════
# Gráficos e CSV
# ══════════════════════════════════════════════════════════════════════

def _export_csv(stamp: str):
    fname = f"results_{stamp}.csv"
    rows  = _prod_metrics + _cons_metrics + _sat_metrics
    if not rows:
        return
    with open(fname, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["phase", "label", "n", "throughput_ops_s",
                    "lat_p50_ms", "lat_p95_ms", "lat_p99_ms", "lat_max_ms",
                    "total", "errors", "success_rate"])
        for m in rows:
            w.writerow([m.phase, m.label, m.n,
                        f"{m.throughput:.2f}", f"{m.lat_p50:.1f}",
                        f"{m.lat_p95:.1f}", f"{m.lat_p99:.1f}", f"{m.lat_max:.1f}",
                        m.total, m.errors, f"{m.success_rate:.3f}"])
    ok(f"Dados brutos → {fname}")
    return fname


def plot_results(stamp: str):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError:
        warn("matplotlib não instalado — sem gráficos. Instala com: pip install matplotlib")
        return

    fig = plt.figure(figsize=(16, 12))
    fig.suptitle(
        f"SD Sistema — Resultados de Performance\n{datetime.datetime.now():%Y-%m-%d %H:%M}",
        fontsize=13, fontweight="bold")

    gs = fig.add_gridspec(2, 2, hspace=0.42, wspace=0.35)

    def _ns(metrics): return [m.n          for m in metrics]
    def _tp(metrics): return [m.throughput for m in metrics]
    def _p50(metrics): return [m.lat_p50   for m in metrics]
    def _p95(metrics): return [m.lat_p95   for m in metrics]
    def _p99(metrics): return [m.lat_p99   for m in metrics]

    # ── Painel [0,0]: throughput produtores ─────────────────────────────
    ax = fig.add_subplot(gs[0, 0])
    if _prod_metrics:
        ns   = _ns(_prod_metrics)
        bars = ax.bar(ns, _tp(_prod_metrics), color="#4C9BE8", width=0.6, zorder=3)
        ax.bar_label(bars, fmt="%.1f", fontsize=8, padding=3)
        ax2 = ax.twinx()
        ax2.plot(ns, _p95(_prod_metrics), "o--", color="#E87D4C",
                 linewidth=1.5, markersize=5, label="p95")
        ax2.plot(ns, _p99(_prod_metrics), "s:",  color="#C0392B",
                 linewidth=1.5, markersize=5, label="p99")
        ax2.set_ylabel("Latência (ms)", fontsize=9)
        ax2.legend(fontsize=8, loc="upper right")
    ax.set_title("Produtores — Throughput & Latência", fontsize=10)
    ax.set_xlabel("Produtores concorrentes", fontsize=9)
    ax.set_ylabel("Throughput (ev/s)", fontsize=9)
    ax.set_xticks(_ns(_prod_metrics) if _prod_metrics else [])
    ax.grid(axis="y", alpha=0.3, zorder=0)

    # ── Painel [0,1]: latência percentis produtores ─────────────────────
    ax = fig.add_subplot(gs[0, 1])
    if _prod_metrics:
        ns = _ns(_prod_metrics)
        ax.plot(ns, _p50(_prod_metrics), "o-",  color="#2ECC71",
                linewidth=2, markersize=6, label="p50")
        ax.plot(ns, _p95(_prod_metrics), "s--", color="#E87D4C",
                linewidth=2, markersize=6, label="p95")
        ax.plot(ns, _p99(_prod_metrics), "^:", color="#C0392B",
                linewidth=2, markersize=6, label="p99")
        ax.fill_between(ns, _p50(_prod_metrics), _p99(_prod_metrics),
                        alpha=0.1, color="#C0392B")
    ax.set_title("Produtores — Percentis de Latência", fontsize=10)
    ax.set_xlabel("Produtores concorrentes", fontsize=9)
    ax.set_ylabel("Latência (ms)", fontsize=9)
    ax.set_xticks(_ns(_prod_metrics) if _prod_metrics else [])
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)

    # ── Painel [1,0]: latência percentis consumidores ───────────────────
    ax = fig.add_subplot(gs[1, 0])
    if _cons_metrics:
        ns = _ns(_cons_metrics)
        ax.plot(ns, _p50(_cons_metrics), "o-",  color="#2ECC71",
                linewidth=2, markersize=6, label="p50")
        ax.plot(ns, _p95(_cons_metrics), "s--", color="#E87D4C",
                linewidth=2, markersize=6, label="p95")
        ax.plot(ns, _p99(_cons_metrics), "^:", color="#C0392B",
                linewidth=2, markersize=6, label="p99")
        ax.fill_between(ns, _p50(_cons_metrics), _p99(_cons_metrics),
                        alpha=0.1, color="#C0392B")
        ax2 = ax.twinx()
        ax2.bar(ns, _tp(_cons_metrics), color="#4C9BE8",
                width=0.6, alpha=0.35, zorder=2, label="throughput")
        ax2.set_ylabel("Throughput (q/s)", fontsize=9, color="#4C9BE8")
        ax2.tick_params(axis="y", labelcolor="#4C9BE8")
    ax.set_title("Consumidores — Latência & Throughput", fontsize=10)
    ax.set_xlabel("Consumidores concorrentes", fontsize=9)
    ax.set_ylabel("Latência (ms)", fontsize=9)
    ax.set_xticks(_ns(_cons_metrics) if _cons_metrics else [])
    ax.legend(fontsize=9, loc="upper left")
    ax.grid(alpha=0.3, zorder=0)

    # ── Painel [1,1]: curva de saturação ────────────────────────────────
    ax = fig.add_subplot(gs[1, 1])
    if _sat_metrics:
        ns  = _ns(_sat_metrics)
        tps = _tp(_sat_metrics)
        p99 = _p99(_sat_metrics)
        p95 = _p95(_sat_metrics)

        bars = ax.bar(ns, tps, color="#4C9BE8", width=[max(1, n * 0.5) for n in ns],
                      alpha=0.75, zorder=3, label="throughput")
        ax.bar_label(bars, fmt="%.1f", fontsize=7, padding=2)

        ax2 = ax.twinx()
        ax2.plot(ns, p95, "s--", color="#E87D4C",
                 linewidth=2, markersize=5, label="p95 lat")
        ax2.plot(ns, p99, "^-",  color="#C0392B",
                 linewidth=2.5, markersize=6, label="p99 lat")
        ax2.axhline(SAT_P99_LIMIT, color="#C0392B", linestyle=":", linewidth=1.5,
                    alpha=0.7, label=f"limite p99 ({SAT_P99_LIMIT}ms)")
        ax2.set_ylabel("Latência (ms)", fontsize=9)
        ax2.legend(fontsize=8, loc="upper left")

        # Anotar ponto de saturação
        sat_idx = next((i for i, m in enumerate(_sat_metrics)
                        if m.lat_p99 > SAT_P99_LIMIT), None)
        if sat_idx is not None:
            xsat = _sat_metrics[sat_idx].n
            ax.axvline(xsat, color="#C0392B", linestyle="--", linewidth=1.5, alpha=0.6)
            ax.text(xsat, max(tps) * 1.05, f" Saturação\n n={xsat}",
                    color="#C0392B", fontsize=8, va="bottom")

    ax.set_title("Curva de Saturação — Consumers", fontsize=10)
    ax.set_xlabel("Consumidores concorrentes", fontsize=9)
    ax.set_ylabel("Throughput (q/s)", fontsize=9)
    ax.set_xticks(_ns(_sat_metrics) if _sat_metrics else [])
    ax.legend(fontsize=9, loc="upper right")
    ax.grid(axis="y", alpha=0.3, zorder=0)

    fname = f"results_{stamp}.png"
    plt.savefig(fname, dpi=150, bbox_inches="tight")
    plt.close()
    ok(f"Gráficos guardados em {fname}")

# ══════════════════════════════════════════════════════════════════════
# Orquestração principal
# ══════════════════════════════════════════════════════════════════════

def main() -> int:
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    h1("Sistema SD — Testes de Escalabilidade e Robustez")
    print(f"  Data: {datetime.date.today()}  |  Seed: {SEED_START} → {SEED_END}")
    print(f"  Níveis produtores: {PROD_LEVELS}  |  Consumidores: {CONS_LEVELS}")
    print(f"  Saturação: {SAT_LEVELS}  (p99 limit={SAT_P99_LIMIT}ms)")

    # ── Build ──────────────────────────────────────────────────────────
    h2("Build")
    if not build_erlang():               sys.exit(1)
    if not build_java(DHT_POM, "DHT/AST"): sys.exit(1)
    if not build_java(SA_POM,  "SA"):       sys.exit(1)

    # ── DHT (2 nós) ───────────────────────────────────────────────────
    h2("A arrancar DHT (2 nós, consistent hashing)")
    launch_java(DHT_POM, "pt.ua.NodeServerMain",
                f"node-1 {DHT1_PORT} 3 {INDEX_FIELDS} node-2:localhost:{DHT2_PORT}", "dht1")
    launch_java(DHT_POM, "pt.ua.NodeServerMain",
                f"node-2 {DHT2_PORT} 3 {INDEX_FIELDS} node-1:localhost:{DHT1_PORT}", "dht2")
    for port, name in [(DHT1_PORT, "DHT-1"), (DHT2_PORT, "DHT-2")]:
        if wait_port(port): ok(f"{name} pronto :{port}")
        else: fail(f"{name} não arrancou em {STARTUP_TIMEOUT}s"); _kill_all(); return 1

    # ── SA (2 nós, cache distribuída) ─────────────────────────────────
    h2("A arrancar SA (2 nós, cache peer-to-peer)")
    launch_java(SA_POM, "pt.ua.SaMain",
                f"north {SA_N_PORT} localhost {SA_S_PORT} localhost {DHT1_PORT}", "sa-north")
    launch_java(SA_POM, "pt.ua.SaMain",
                f"south {SA_S_PORT} localhost {SA_N_PORT} localhost {DHT1_PORT}", "sa-south")
    for port, name in [(SA_N_PORT, "SA-north"), (SA_S_PORT, "SA-south")]:
        if wait_port(port): ok(f"{name} pronto :{port}")
        else: fail(f"{name} não arrancou em {STARTUP_TIMEOUT}s"); _kill_all(); return 1

    # ── SS (2 nós Erlang + ZMQ gossip) ────────────────────────────────
    h2("A arrancar SS (2 nós Erlang, gossip ZMQ)")
    launch_ss("north", SS_N_PORT, GOSSIP_N, GOSSIP_S, SA_N_PORT)
    launch_ss("south", SS_S_PORT, GOSSIP_S, GOSSIP_N, SA_S_PORT)
    for port, name in [(SS_N_PORT, "SS-north"), (SS_S_PORT, "SS-south")]:
        if wait_port(port): ok(f"{name} pronto :{port}")
        else: fail(f"{name} não arrancou em {STARTUP_TIMEOUT}s"); _kill_all(); return 1

    time.sleep(GOSSIP_SETTLE)

    # ── Seed de dados históricos ───────────────────────────────────────
    h2("Seed de dados históricos (FakeSSMain — intervalo largo)")
    seed_data()

    # ── Fases de teste ────────────────────────────────────────────────
    test_smoke()
    test_state()
    test_aggregations()
    test_producers()
    test_consumers()
    test_saturation()
    test_robustness()

    # ── Exportar métricas e gráficos ──────────────────────────────────
    h2("A gerar resultados")
    _export_csv(stamp)
    plot_results(stamp)

    # ── Relatório final ───────────────────────────────────────────────
    h1("Relatório Final")
    passed = sum(1 for _, p, _ in _results if p)
    total  = len(_results)
    for name, p, detail in _results:
        (ok if p else fail)(f"{name:<60} {detail}")
    print(f"\n  {'='*68}")
    print(f"  {_c('1', f'Resultado: {passed}/{total} testes passaram')}")
    if passed == total:
        print(f"  {_c('32', 'TODOS OS TESTES PASSARAM')}")
    else:
        print(f"  {_c('31', f'{total - passed} TESTES FALHARAM')}")
    print()

    _kill_all()
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
