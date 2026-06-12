%%%-------------------------------------------------------------------
%%% ss_app — Arranque da aplicação OTP (behaviour application).
%%%-------------------------------------------------------------------
-module(ss_app).
-behaviour(application).
-export([start/2, stop/1]).

start(_Type, _Args) ->
    ss_sup:start_link().

stop(_State) ->
    ok.
