%%% @doc Functions for escaping and unescaping mixed case values, for use in HTTP
%%% headers. Both percent-encoding and escaping of double-quoted strings
%%% (`"' => `\"') are supported.
%%%
%%% This is necessary for encodings of AO-Core messages for transmission in
%%% HTTP/2 and HTTP/3, because uppercase header keys are explicitly disallowed.
%%% While most map keys in HyperBEAM are normalized to lowercase, IDs are not.
%%% Subsequently, we encode all header keys to lowercase %-encoded URI-style
%%% strings because transmission.
-module(hb_escape).
-export([encode/1, decode/1, encode_keys/2, decode_keys/2]).
-export([encode_header/1, decode_header/1]).
-export([encode_quotes/1, decode_quotes/1]).
-export([encode_ampersand/1]).
-include_lib("eunit/include/eunit.hrl").
-include("include/hb.hrl").

%% @doc Encode a binary as a URI-encoded string.
encode(Bin) when is_binary(Bin) ->
    case uri_safe(Bin) of
        true -> Bin;
        false -> iolist_to_binary(lists:reverse(percent_escape(Bin, [])))
    end.

%% @doc Decode a URI-encoded string back to a binary.
decode(Bin) when is_binary(Bin) ->
    decode(Bin, Bin).
decode(<<>>, Original) ->
    Original;
decode(<<$%, _/binary>>, Original) ->
    iolist_to_binary(lists:reverse(percent_unescape(Original, [])));
decode(<<_C, Rest/binary>>, Original) ->
    decode(Rest, Original).

%% @doc Escape `\\', CR and LF for a header value: `\\' -> `\\\\',
%% CR -> `\\r', LF -> `\\n'; every other byte is copied.
%%
%% Linear in the value: the escapable bytes are found by `binary:matches/2' and
%% the untouched runs between them are sub-binaries of the input, joined once.
%% (The first version rebuilt the whole tail at every byte -- quadratic, and a
%% non-tail call per byte -- which for a multi-megabyte value was seconds.) A
%% value with nothing to escape is returned as is.
encode_header(Bin) when is_binary(Bin) ->
    case binary:matches(Bin, header_escapes()) of
        [] -> Bin;
        Found -> iolist_to_binary(encode_header_runs(Bin, 0, Found))
    end.

encode_header_runs(Bin, Pos, []) ->
    [binary:part(Bin, Pos, byte_size(Bin) - Pos)];
encode_header_runs(Bin, Pos, [{At, 1} | Rest]) ->
    [binary:part(Bin, Pos, At - Pos), header_escape(binary:at(Bin, At))
    | encode_header_runs(Bin, At + 1, Rest)].

header_escape($\\) -> <<"\\\\">>;
header_escape($\r) -> <<"\\r">>;
header_escape($\n) -> <<"\\n">>.

header_escapes() ->
    case persistent_term:get({?MODULE, header_escapes}, undefined) of
        undefined ->
            CP = binary:compile_pattern([<<"\\">>, <<"\r">>, <<"\n">>]),
            persistent_term:put({?MODULE, header_escapes}, CP),
            CP;
        CP -> CP
    end.

%% @doc The inverse of `encode_header/1': `\\\\' -> `\\', `\\r' -> CR,
%% `\\n' -> LF. A backslash followed by anything else (or by nothing) is
%% kept, and the byte after it is read as an ordinary byte. Linear, as
%% `encode_header/1'.
decode_header(Bin) when is_binary(Bin) ->
    case binary:match(Bin, <<"\\">>) of
        nomatch -> Bin;
        {At, 1} -> iolist_to_binary(decode_header_runs(Bin, 0, At))
    end.

decode_header_runs(Bin, Pos, nomatch) ->
    [binary:part(Bin, Pos, byte_size(Bin) - Pos)];
decode_header_runs(Bin, Pos, At) ->
    Size = byte_size(Bin),
    Run = binary:part(Bin, Pos, At - Pos),
    {Out, Next} =
        case At + 1 < Size of
            false -> {<<"\\">>, At + 1};
            true ->
                case binary:at(Bin, At + 1) of
                    $\\ -> {<<"\\">>, At + 2};
                    $r -> {<<"\r">>, At + 2};
                    $n -> {<<"\n">>, At + 2};
                    _ -> {<<"\\">>, At + 1}
                end
        end,
    [Run, Out | decode_header_runs(Bin, Next, next_backslash(Bin, Next))].

next_backslash(Bin, Pos) when Pos >= byte_size(Bin) -> nomatch;
next_backslash(Bin, Pos) ->
    case binary:match(Bin, <<"\\">>, [{scope, {Pos, byte_size(Bin) - Pos}}]) of
        nomatch -> nomatch;
        {At, 1} -> At
    end.

%% @doc Encode a string with escaped quotes.
encode_quotes(String) when is_binary(String) ->
    list_to_binary(encode_quotes(binary_to_list(String)));
encode_quotes([]) -> [];
encode_quotes([$\" | Rest]) -> [$\\, $\" | encode_quotes(Rest)];
encode_quotes([C | Rest]) -> [C | encode_quotes(Rest)].

%% @doc Decode a string with escaped quotes.
decode_quotes(String) when is_binary(String) ->
    list_to_binary(decode_quotes(binary_to_list(String)));
decode_quotes([]) -> [];
decode_quotes([$\\, $\" | Rest]) -> [$\" | decode_quotes(Rest)];
decode_quotes([$\" | Rest]) -> decode_quotes(Rest);
decode_quotes([C | Rest]) -> [C | decode_quotes(Rest)].

%% @doc Encode ampersands as &amp; for XML output.
encode_ampersand(String) when is_binary(String) ->
    list_to_binary(encode_ampersand(binary_to_list(String)));
encode_ampersand([]) -> [];
encode_ampersand([$& | Rest]) -> [$&, $a, $m, $p, $; | encode_ampersand(Rest)];
encode_ampersand([C | Rest]) -> [C | encode_ampersand(Rest)].

%% @doc Return a message with all of its keys decoded.
decode_keys(Msg, Opts) when is_map(Msg) ->
    hb_maps:from_list(
        lists:map(
            fun({Key, Value}) -> {decode(Key), Value} end,
            hb_maps:to_list(Msg, Opts)
        )
    );
decode_keys(Other, _Opts) -> Other.

%% @doc URI encode keys in the base layer of a message. Does not recurse.
encode_keys(Msg, Opts) when is_map(Msg) ->
    hb_maps:from_list(
        lists:map(
            fun({Key, Value}) -> {encode(Key), Value} end,
            hb_maps:to_list(Msg, Opts)
        )
    );
encode_keys(Other, _Opts) -> Other.

%% @doc Escape a binary as a URI-encoded string.
uri_safe(<<>>) -> true;
uri_safe(<<C, Rest/binary>>) when C >= $a, C =< $z ->
    uri_safe(Rest);
uri_safe(<<C, Rest/binary>>) when C >= $0, C =< $9 ->
    uri_safe(Rest);
uri_safe(<<C, Rest/binary>>) when
        C == $.; C == $-; C == $_; C == $/;
        C == $?; C == $& ->
    uri_safe(Rest);
uri_safe(_) ->
    false.

percent_escape(<<>>, Acc) -> Acc;
percent_escape(<<C, Rest/binary>>, Acc) when C >= $a, C =< $z ->
    percent_escape(Rest, [C | Acc]);
percent_escape(<<C, Rest/binary>>, Acc) when C >= $0, C =< $9 ->
    percent_escape(Rest, [C | Acc]);
percent_escape(<<C, Rest/binary>>, Acc) when
        C == $.; C == $-; C == $_; C == $/;
        C == $?; C == $& ->
    percent_escape(Rest, [C | Acc]);
percent_escape(<<C, Rest/binary>>, Acc) ->
    percent_escape(Rest, [escape_byte(C) | Acc]).

%% @doc Escape a single byte as a URI-encoded string.
escape_byte(C) when C >= 0, C =< 255 ->
    [$%, hex_digit(C bsr 4), hex_digit(C band 15)].

hex_digit(N) when N >= 0, N =< 9 ->
    N + $0;
hex_digit(N) when N > 9, N =< 15 ->
    N + $a - 10.

%% @doc Unescape a URI-encoded string.
percent_unescape(<<$%, H1, H2, Rest/binary>>, Acc) ->
    Byte = (hex_value(H1) bsl 4) + hex_value(H2),
    percent_unescape(Rest, [Byte | Acc]);
percent_unescape(<<C, Rest/binary>>, Acc) ->
    percent_unescape(Rest, [C | Acc]);
percent_unescape(<<>>, Acc) ->
    Acc.

hex_value(C) when C >= $0, C =< $9 ->
    C - $0;
hex_value(C) when C >= $a, C =< $f ->
    C - $a + 10;
hex_value(C) when C >= $A, C =< $F ->
    C - $A + 10.

%%% Tests

escape_unescape_identity_test() ->
    % Test that unescape(escape(X)) == X for various inputs
    TestCases = [
        <<"hello">>,
        <<"hello, world!">>,
        <<"hello+list">>,
        <<"special@chars#here">>,
        <<"UPPERCASE">>,
        <<"MixedCASEstring">>,
        <<"12345">>,
        <<>> % Empty string
    ],
    ?event(parsing,
        {escape_unescape_identity_test,
            {test_cases,
                [
                        {Case, {explicit, encode(Case)}}
                    ||
                        Case <- TestCases
                ]
            }
        }
    ),
    lists:foreach(fun(TestCase) ->
        ?assertEqual(TestCase, decode(encode(TestCase)))
    end, TestCases).

unescape_specific_test() ->
    % Test specific unescape cases
    ?assertEqual(<<"a">>, decode(<<"%61">>)),
    ?assertEqual(<<"A">>, decode(<<"%41">>)),
    ?assertEqual(<<"!">>, decode(<<"%21">>)),
    ?assertEqual(<<"hello, World!">>, decode(<<"hello%2c%20%57orld%21">>)),
    ?assertEqual(<<"/">>, decode(<<"%2f">>)),
    ?assertEqual(<<"?">>, decode(<<"%3f">>)).

uppercase_test() ->
    % Test uppercase characters are properly escaped
    ?assertEqual(<<"%41">>, encode(<<"A">>)),
    ?assertEqual(<<"%42">>, encode(<<"B">>)),
    ?assertEqual(<<"%5a">>, encode(<<"Z">>)),
    ?assertEqual(<<"hello%20%57orld">>, encode(<<"hello World">>)),
    ?assertEqual(<<"test%41%42%43">>, encode(<<"testABC">>)).

escape_unescape_special_chars_test() ->
    % Test characters that should be escaped
    SpecialChars = [
        $@, $#, $", $$, $%, $&, $', $(, $), $*, $+, $,, $/, $:, $;, 
        $<, $=, $>, $?, $[, $\\, $], $^, $`, ${, $|, $}, $~, $\s
    ],
    TestString = list_to_binary(SpecialChars),
    ?assertEqual(TestString, decode(encode(TestString))).

%%% Header escaping: the linear implementations against the original
%%% byte-at-a-time definitions, kept here as the reference.

ref_encode_header(<<>>) -> <<>>;
ref_encode_header(<<$\\, Rest/binary>>) -> <<"\\\\", (ref_encode_header(Rest))/binary>>;
ref_encode_header(<<$\r, Rest/binary>>) -> <<"\\r", (ref_encode_header(Rest))/binary>>;
ref_encode_header(<<$\n, Rest/binary>>) -> <<"\\n", (ref_encode_header(Rest))/binary>>;
ref_encode_header(<<C, Rest/binary>>) -> <<C, (ref_encode_header(Rest))/binary>>.

ref_decode_header(<<>>) -> <<>>;
ref_decode_header(<<$\\, $\\, Rest/binary>>) -> <<$\\, (ref_decode_header(Rest))/binary>>;
ref_decode_header(<<$\\, $r, Rest/binary>>) -> <<$\r, (ref_decode_header(Rest))/binary>>;
ref_decode_header(<<$\\, $n, Rest/binary>>) -> <<$\n, (ref_decode_header(Rest))/binary>>;
ref_decode_header(<<C, Rest/binary>>) -> <<C, (ref_decode_header(Rest))/binary>>.

%% A random binary of `Len' bytes, drawn either from every byte value or
%% from an alphabet dense in the bytes the escaping cares about.
header_random(Len) ->
    Alphabet = <<"\\\r\nrnab\"%">>,
    case rand:uniform(2) of
        1 -> crypto:strong_rand_bytes(Len);
        2 ->
            << <<(binary:at(Alphabet, rand:uniform(byte_size(Alphabet)) - 1))>>
            || _ <- lists:seq(1, Len) >>
    end.

header_escape_edge_cases_test() ->
    Cases = [<<>>, <<"\\">>, <<"\r">>, <<"\n">>, <<"\\\\">>, <<"\\r">>,
             <<"\\n">>, <<"\\x">>, <<"x\\">>, <<"\\\\\\">>, <<"\r\n">>,
             <<"abc">>, << <<B>> || B <- lists:seq(0, 255) >>],
    lists:foreach(
        fun(C) ->
            ?assertEqual(ref_encode_header(C), encode_header(C)),
            ?assertEqual(ref_decode_header(C), decode_header(C)),
            ?assertEqual(C, decode_header(encode_header(C)))
        end,
        Cases).

header_escape_matches_reference_test_() ->
    {timeout, 120, fun() ->
        Lens = [ rand:uniform(64) - 1 || _ <- lists:seq(1, 3000) ] ++
               [ rand:uniform(4096) || _ <- lists:seq(1, 300) ] ++
               [ 20000 + rand:uniform(20000) || _ <- lists:seq(1, 6) ],
        lists:foreach(
            fun(Len) ->
                B = header_random(Len),
                E = encode_header(B),
                ?assertEqual(ref_encode_header(B), E),
                ?assertEqual(ref_decode_header(B), decode_header(B)),
                ?assertEqual(ref_decode_header(E), decode_header(E)),
                ?assertEqual(B, decode_header(E))
            end,
            Lens)
    end}.

%% A long value is linear: 4 MB with escapes throughout encodes and decodes
%% in well under a second (the reference took minutes at this size).
header_escape_long_value_test_() ->
    {timeout, 60, fun() ->
        B = header_random(4 * 1024 * 1024),
        {T1, E} = timer:tc(fun() -> encode_header(B) end),
        {T2, D} = timer:tc(fun() -> decode_header(E) end),
        ?assertEqual(B, D),
        ?assert(T1 + T2 < 5000000)
    end}.
