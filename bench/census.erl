Store = #{ <<"store-module">> => hb_store_lmdb, <<"name">> => <<"cache-mainnet/lmdb">> },
#{ <<"db">> := DB } = hb_store:find(Store), elmdb:flush(DB),
Seg = fun(S) ->
    case S of
        _ when byte_size(S) == 43 -> <<"<id>">>;
        _ ->
            case re:run(S, <<"^[0-9]+$">>) of
                {match, _} -> <<"<n>">>;
                _ -> case byte_size(S) > 24 of true -> <<"<long>">>; false -> S end
            end
    end
end,
Cat = fun(K) ->
    Parts = binary:split(K, <<"/">>, [global]),
    Norm = [Seg(P) || P <- lists:sublist(Parts, 4)],
    iolist_to_binary(lists:join(<<"/">>, Norm ++ [integer_to_binary(length(Parts))]))
end,
{ok, Acc} = elmdb:fold(DB, fun(K, V, A) ->
    C = Cat(K), {N, B} = maps:get(C, A, {0, 0}),
    A#{ C => {N + 1, B + byte_size(K) + byte_size(V)} }
end, #{}),
file:write_file(os:getenv("OUT", "/tmp/census.txt"), io_lib:format("~p.~n", [Acc])),
maps:size(Acc).
