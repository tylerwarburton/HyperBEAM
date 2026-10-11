-module(luerl_gc_detcheck).
%% Lockstep: one VM collected by the previous luerl_heap:gc/1 (compiled from
%% the old patch's luerl_heap.erl with its module renamed luerl_heap_stock),
%% one by the candidate (loaded as luerl_heap). After every message both VMs
%% must be the same term, byte for byte, and must have returned the same
%% values (which expose table-store indices via tostring and the order of
%% table keys). Run: `luerl_gc_detcheck:main(130)'. The workload grows
%% superlinearly; a few hundred messages is plenty.
%%
%% Compile both luerl_heap copies with the options luerl itself was built
%% with (`proplists:get_value(options, luerl_heap:module_info(compile))'):
%% a plain `erlc' lacks `HAS_FULL_KEYS' and silently builds the array table
%% store instead of the maps one.
-export([main/0, main/1]).

script() -> <<"
Pool = {}
Keys = {}
seed = 1
local function rnd(n) seed = (seed * 1103515245 + 12345) % 2147483648; return seed % n + 1 end
function step(k)
  local out = {}
  for j = 1, 200 do
    local r = rnd(10)
    if r <= 4 then
      local t = { k = k, j = j, inner = { j }, f = function() return j end }
      Pool[rnd(500)] = t
    elseif r <= 6 then
      Pool[rnd(500)] = nil
    elseif r <= 8 then
      local key = {}
      Keys[key] = rnd(1000)
      if rnd(4) == 1 then for kk in pairs(Keys) do Keys[kk] = nil; break end end
    else
      local tmp = {}
      for i = 1, 20 do tmp[i] = { i } end
    end
  end
  local new = {}
  out[#out + 1] = tostring(new)
  for kk, v in pairs(Keys) do out[#out + 1] = tostring(kk) .. '=' .. v end
  return table.concat(out, ',')
end
">>.

main() -> main(400).
main(N) ->
    St0 = luerl:init(),
    {ok, _, St1} = luerl:do(script(), St0),
    true = lists:member({seen_new, 1}, luerl_heap:module_info(functions)),
    put(t0, erlang:monotonic_time(millisecond)),
    loop(1, N, St1, St1),
    io:format("DETCHECK OK: ~p messages, identical VMs and outputs~n", [N]).

loop(K, N, _A, _B) when K > N -> ok;
loop(K, N, A0, B0) ->
    {ok, RA, A1} = luerl:call_function([<<"step">>], [K], A0),
    {ok, RB, B1} = luerl:call_function([<<"step">>], [K], B0),
    RA = RB,
    A2 = luerl_heap_stock:gc(A1),
    B2 = luerl_heap:gc(B1),
    case term_to_binary(A2) =:= term_to_binary(B2) of
        true -> ok;
        false -> io:format("DIVERGED at ~p~n", [K]), halt(1)
    end,
    case K rem 10 of 0 -> io:format("~p ~p ms~n", [K, erlang:monotonic_time(millisecond) - get(t0)]); _ -> ok end,
    loop(K + 1, N, A2, B2).
