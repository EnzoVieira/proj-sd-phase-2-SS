%%%-------------------------------------------------------------------
%%% ss_json — Codec JSON mínimo (Fase 1A)
%%%
%%% Converte entre texto JSON (binary) e termos Erlang:
%%%   objeto JSON  <-> map com chaves binary   (ex: #{<<"cmd">> => <<"event">>})
%%%   array  JSON  <-> lista                    (ex: [1, 2, 3])
%%%   string JSON  <-> binary                   (ex: <<"alarme">>)
%%%   número JSON  <-> integer ou float
%%%   true/false/null <-> átomos true/false/null
%%%
%%% Suficiente para o nosso protocolo (objetos e arrays, valores string/número/
%%% bool). NÃO trata escapes unicode \uXXXX (não precisamos no protocolo).
%%%
%%% CONCEITOS NOVOS: binaries e pattern matching sobre binaries; funções que
%%% devolvem {Valor, Resto} para irem "consumindo" a entrada; iolists.
%%%-------------------------------------------------------------------
-module(ss_json).
-export([decode/1, encode/1]).

%%====================================================================
%% DECODE: binary JSON -> termo Erlang
%%====================================================================

decode(Bin) when is_binary(Bin) ->
    %% parse_value devolve {Valor, Resto}. Depois de ler o valor de topo,
    %% só deve sobrar espaço em branco.
    {Value, Rest} = parse_value(skip_ws(Bin)),
    case skip_ws(Rest) of
        <<>> -> Value;
        Extra -> error({json_trailing_data, Extra})
    end.

%% skip_ws/1 — descarta espaços, tabs e mudanças de linha no início.
%% Note como cada cláusula faz match a um caractere específico e recursa.
skip_ws(<<$\s, Rest/binary>>) -> skip_ws(Rest);
skip_ws(<<$\t, Rest/binary>>) -> skip_ws(Rest);
skip_ws(<<$\n, Rest/binary>>) -> skip_ws(Rest);
skip_ws(<<$\r, Rest/binary>>) -> skip_ws(Rest);
skip_ws(Bin) -> Bin.

%% parse_value/1 — olha para o início e decide que tipo de valor é.
%% A ORDEM das cláusulas importa: o Erlang tenta de cima para baixo.
parse_value(<<"true", Rest/binary>>)  -> {true, Rest};
parse_value(<<"false", Rest/binary>>) -> {false, Rest};
parse_value(<<"null", Rest/binary>>)  -> {null, Rest};
parse_value(<<$", Rest/binary>>)      -> parse_string(Rest, []);
parse_value(<<${, Rest/binary>>)      -> parse_object(skip_ws(Rest), #{});
parse_value(<<$[, Rest/binary>>)      -> parse_array(skip_ws(Rest), []);
parse_value(Bin)                       -> parse_number(Bin).

%% parse_string/2 — acumula caracteres até encontrar o " de fecho.
%% Acc é uma lista de bytes ao contrário (mais eficiente prepender à cabeça);
%% no fim invertemos e transformamos em binary.
parse_string(<<$", Rest/binary>>, Acc) ->
    {list_to_binary(lists:reverse(Acc)), Rest};
parse_string(<<$\\, C, Rest/binary>>, Acc) ->        %% caractere escapado: \X
    parse_string(Rest, [unescape(C) | Acc]);
parse_string(<<C, Rest/binary>>, Acc) ->
    parse_string(Rest, [C | Acc]).

unescape($n)  -> $\n;
unescape($t)  -> $\t;
unescape($r)  -> $\r;
unescape($")  -> $";
unescape($\\) -> $\\;
unescape($/)  -> $/;
unescape(C)   -> C.

%% parse_object/2 — lê pares "chave":valor separados por vírgula, até ao }.
parse_object(<<$}, Rest/binary>>, Acc) ->
    {Acc, Rest};
parse_object(<<$", Rest0/binary>>, Acc) ->
    {Key, Rest1} = parse_string(Rest0, []),          %% a chave é uma string
    <<$:, Rest2/binary>> = skip_ws(Rest1),           %% tem de vir um ':'
    {Val, Rest3} = parse_value(skip_ws(Rest2)),      %% depois o valor
    case skip_ws(Rest3) of
        <<$,, Rest4/binary>> -> parse_object(skip_ws(Rest4), maps:put(Key, Val, Acc));
        <<$}, Rest4/binary>> -> {maps:put(Key, Val, Acc), Rest4}
    end.

%% parse_array/2 — lê valores separados por vírgula, até ao ].
parse_array(<<$], Rest/binary>>, Acc) ->
    {lists:reverse(Acc), Rest};
parse_array(Bin, Acc) ->
    {Val, Rest1} = parse_value(Bin),
    case skip_ws(Rest1) of
        <<$,, Rest2/binary>> -> parse_array(skip_ws(Rest2), [Val | Acc]);
        <<$], Rest2/binary>> -> {lists:reverse([Val | Acc]), Rest2}
    end.

%% parse_number/1 — recolhe os caracteres que formam o número e converte.
parse_number(Bin) ->
    {RevDigits, Rest} = take_number(Bin, []),
    Str = lists:reverse(RevDigits),
    Value =
        case lists:member($., Str) orelse lists:member($e, Str) orelse lists:member($E, Str) of
            true  -> list_to_float(Str);
            false -> list_to_integer(Str)
        end,
    {Value, Rest}.

take_number(<<C, Rest/binary>>, Acc)
  when (C >= $0 andalso C =< $9);
       C =:= $-; C =:= $+; C =:= $.; C =:= $e; C =:= $E ->
    take_number(Rest, [C | Acc]);
take_number(Bin, Acc) ->
    {Acc, Bin}.

%%====================================================================
%% ENCODE: termo Erlang -> binary JSON
%%====================================================================

%% Construímos uma "iolist" (lista aninhada de binaries/bytes) — é mais
%% eficiente do que concatenar strings — e no fim achatamos para um binary.
encode(Term) ->
    iolist_to_binary(enc(Term)).

enc(true)  -> <<"true">>;
enc(false) -> <<"false">>;
enc(null)  -> <<"null">>;
enc(I) when is_integer(I) -> integer_to_binary(I);
enc(F) when is_float(F)   -> float_to_binary(F, [{decimals, 6}, compact]);
enc(B) when is_binary(B)  -> enc_string(B);
enc(A) when is_atom(A)    -> enc_string(atom_to_binary(A, utf8));
enc(L) when is_list(L)    -> ["[", join([enc(X) || X <- L], ","), "]"];
enc(M) when is_map(M)     ->
    Pairs = [ [enc_string(to_bin(K)), ":", enc(V)] || {K, V} <- maps:to_list(M) ],
    ["{", join(Pairs, ","), "}"].

enc_string(B) -> ["\"", escape(B), "\""].

escape(B) -> escape(B, []).
escape(<<>>, Acc)                  -> lists:reverse(Acc);
escape(<<$", Rest/binary>>, Acc)   -> escape(Rest, [<<"\\\"">> | Acc]);
escape(<<$\\, Rest/binary>>, Acc)  -> escape(Rest, [<<"\\\\">> | Acc]);
escape(<<$\n, Rest/binary>>, Acc)  -> escape(Rest, [<<"\\n">> | Acc]);
escape(<<$\t, Rest/binary>>, Acc)  -> escape(Rest, [<<"\\t">> | Acc]);
escape(<<$\r, Rest/binary>>, Acc)  -> escape(Rest, [<<"\\r">> | Acc]);
escape(<<C, Rest/binary>>, Acc)    -> escape(Rest, [C | Acc]).

%% join/2 — intercala um separador entre os elementos de uma lista.
join([], _Sep)        -> [];
join([X], _Sep)       -> [X];
join([X | Xs], Sep)   -> [X, Sep | join(Xs, Sep)].

to_bin(B) when is_binary(B) -> B;
to_bin(A) when is_atom(A)   -> atom_to_binary(A, utf8);
to_bin(L) when is_list(L)   -> list_to_binary(L).
