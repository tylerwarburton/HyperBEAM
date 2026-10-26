%%% @doc The device of the `ni:' URI scheme (RFC 6920). A `ni:' URI identifies
%%% a binary by its SHA-256: `ni:///sha-256;<base64url>', with no authority.
%%% The SHA-256 is the `id' that `message@1.0' gives the binary. `hb_cache'
%%% stores each binary under its `ni:' URI.
%%%
%%% `from' with `type: uri' reads the binary of a `ni:' URI from the cache.
%%% `to' with `type: uri' writes the `ni:' URI of a binary.
-module(dev_ni).
-export([from/3, to/3]).
-include("include/hb.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc Loads the binary that a `ni:' URI identifies, from the cache.
-spec from(
    #{ body := binary(), _ => _ },
    #{ type := binary(), _ => _ },
    #{ _ => _ }
) -> {ok, binary()} | {error, _}.
from(#{ <<"body">> := Body }, #{ <<"type">> := <<"uri">> }, Opts) ->
    hb_cache:read(<<"ni:", Body/binary>>, Opts).

%% @doc Writes the `ni:' URI of a binary.
-spec to(
    binary(),
    #{ type := binary(), _ => _ },
    #{ _ => _ }
) -> {ok, binary()} | {error, _}.
to(Bin, #{ <<"type">> := <<"uri">> }, Opts) ->
    {ok, ?NI_URI(hb_path:hashpath(Bin, Opts))}.

%%% Tests

%% @doc Writes the `ni:' URI of `Hello World!', which is RFC 6920 s8.1's
%% example. Reads the binary back with the URI as a base, as a path and as a
%% link value.
uri_test() ->
    Opts = #{ <<"store">> => hb_test_utils:test_store() },
    Bin = <<"Hello World!">>,
    ID = <<"f4OxZX_x_FO5LcGBSKHWXfwtSx-j1ncoSt3SABJtkGk">>,
    URI = <<"ni:///sha-256;", ID/binary>>,
    {ok, _} = hb_cache:write(Bin, Opts),
    ?assertEqual(
        {ok, URI},
        hb_uri:to(Bin, #{ <<"scheme">> => <<"ni">> }, Opts)
    ),
    ?assertEqual({ok, Bin}, hb_ao:resolve_many([URI], Opts)),
    ?assertEqual(
        {ok, Bin},
        hb_ao:resolve(<<"/ni:%2f%2f%2fsha-256;", ID/binary>>, Opts)
    ),
    ?assertEqual(
        {ok, Bin},
        hb_ao:resolve(#{ <<"k">> => {link, URI, #{}} }, <<"k">>, Opts)
    ).
