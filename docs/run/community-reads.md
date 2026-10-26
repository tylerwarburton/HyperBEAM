# Verified community reads

Use one `hb_store_remote_node` store per peer. It verifies the requested ID
and the returned commitments before accepting the value. An ordinary store
list tries peers in order; `hb_store_multi` tries them in parallel and returns
the first verified result.

```erlang
Peer = #{
    <<"store-module">> => hb_store_remote_node,
    <<"node">> => PeerURL
},
Backstops = #{
    <<"store-module">> => hb_store_multi,
    <<"name">> => <<"community-backstops">>,
    <<"scope">> => remote,
    <<"stores">> => [Peer]
}.
```

Add peers to `stores` and put `Backstops` after the node's local stores.
Keep `trusted` unset: non-ID paths describe a peer's state and cannot be
verified against a requested content ID.

## HTTP receipts

HTTP reporting is configured through the node's existing `on` message:

```erlang
#{
    <<"on">> => #{
        <<"http-client-receipt">> => ReceiptHandler,
        <<"http-client-admissible-receipt">> => AdmissibleHandler
    }
}
```

Each is an ordinary hook handler or list of handlers, not a singleton path.
Both receive a node-signed receipt in their request's `body`, run
asynchronously, and cannot replace the HTTP result.

- `http-client-receipt` describes each transport attempt, including retries
  and failures. It contains `request-method`, `request-path`, `status-class`
  and `duration` (Erlang native time units), with `path: duration`.
- `http-client-admissible-receipt` describes a response selected by HTTP
  multirequest after its configured `admissible-status` and `admissible`
  checks pass. It contains the original `request-method` and `request-path`,
  the dispatched `request` message, and the full decoded `response`.

Both include `reference` when `http-reference` is configured for the peer.
The reference comes from the node configuration, not the remote response.
The signer is the requesting node, not the peer.

Admissibility means precisely the configured checks. A status-only check does
not verify content. An application rewarding content delivery must configure
a predicate that checks the requested ID and commitments before reporting
acceptance. The store's own verification remains mandatory, independent of
these hooks; a direct remote-store read does not emit the multirequest hook.

Serial and parallel HTTP multirequests report selected responses only.
Responses discarded after the requested number has been selected do not earn
an admissible receipt, though their completed transport attempts may report.
Repeated requests generate new receipts. Neither hook proves payment, mining,
durability, or hardware attestation.

Keep handler implementations locally available. Disable these hooks on routes
used by the handlers, including the collector's route for HTTP report delivery:

```erlang
<<"opts">> => #{
    <<"on">> => #{
        <<"http-client-receipt">> => [],
        <<"http-client-admissible-receipt">> => []
    }
}
```

An empty `on` message does not clear handlers when route options are merged.
The empty handler lists keep report delivery from reporting itself. The
collector owns reporter authorization, deduplication and reward policy;
neither hook is a durable delivery queue.

## Local verification

```sh
HB_PORT=0 rebar3 device test --module hb_gateway_read_examples,hb_http_receipt_examples
```

The examples use real HyperBEAM listeners with independent stores and wallets,
including invalid content, serial/parallel selection and a separate collector.
