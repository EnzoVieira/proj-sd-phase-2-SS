%%%-------------------------------------------------------------------
%%% ss_json — Codec JSON mínimo.
%%% Converte entre texto JSON (binary) e termos Erlang:
%%%   objeto <-> map (chaves binary)   array <-> lista   string <-> binary
%%%   número <-> integer/float         true/false/null <-> átomos
%%% Suficiente para o protocolo (objetos/arrays planos); não trata \uXXXX.
%%%-------------------------------------------------------------------
-module(ss_json).
-export([decode/1, encode/1]).

%%====================================================================
%% DECODE: binary JSON -> termo Erlang
%%====================================================================

decode(Bin) when is_binary(Bin) ->
    {Value, Rest} = parse_value(skip_ws(Bin)),
    case skip_ws(Rest) of
        <<>> -> Value;
        Extra -> error({json_trailing_data, Extra})
    end.

skip_ws(<<$\s, Rest/binary>>) -> skip_ws(Rest);
skip_ws(<<$\t, Rest/binary>>) -> skip_ws(Rest);
skip_ws(<<$\n, Rest/binary>>) -> skip_ws(Rest);
skip_ws(<<$\r, Rest/binary>>) -> skip_ws(Rest);
skip_ws(Bin) -> Bin.

parse_value(<<"true", Rest/binary>>)  -> {true, Rest};
parse_value(<<"false", Rest/binary>>) -> {false, Rest};
parse_value(<<"null", Rest/binary>>)  -> {null, Rest};
parse_value(<<$", Rest/binary>>)      -> parse_string(Rest, []);
parse_value(<<${, Rest/binary>>)      -> parse_object(skip_ws(Rest), #{});
parse_value(<<$[, Rest/binary>>)      -> parse_array(skip_ws(Rest), []);
parse_value(Bin)                       -> parse_number(Bin).

parse_string(<<$", Rest/binary>>, Acc) ->
    {list_to_binary(lists:reverse(Acc)), Rest};
parse_string(<<$\\, C, Rest/binary>>, Acc) ->
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

parse_object(<<$}, Rest/binary>>, Acc) ->
    {Acc, Rest};
parse_object(<<$", Rest0/binary>>, Acc) ->
    {Key, Rest1} = parse_string(Rest0, []),
    <<$:, Rest2/binary>> = skip_ws(Rest1),
    {Val, Rest3} = parse_value(skip_ws(Rest2)),
    case skip_ws(Rest3) of
        <<$,, Rest4/binary>> -> parse_object(skip_ws(Rest4), maps:put(Key, Val, Acc));
        <<$}, Rest4/binary>> -> {maps:put(Key, Val, Acc), Rest4}
    end.

parse_array(<<$], Rest/binary>>, Acc) ->
    {lists:reverse(Acc), Rest};
parse_array(Bin, Acc) ->
    {Val, Rest1} = parse_value(Bin),
    case skip_ws(Rest1) of
        <<$,, Rest2/binary>> -> parse_array(skip_ws(Rest2), [Val | Acc]);
        <<$], Rest2/binary>> -> {lists:reverse([Val | Acc]), Rest2}
    end.

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

join([], _Sep)        -> [];
join([X], _Sep)       -> [X];
join([X | Xs], Sep)   -> [X, Sep | join(Xs, Sep)].

to_bin(B) when is_binary(B) -> B;
to_bin(A) when is_atom(A)   -> atom_to_binary(A, utf8);
to_bin(L) when is_list(L)   -> list_to_binary(L).
