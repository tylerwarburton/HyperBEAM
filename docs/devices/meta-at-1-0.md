# Device: ~meta@1.0

## Overview

The [`~meta@1.0`](../resources/source-code/dev_meta.md) device provides access to metadata and configuration information about the local HyperBEAM node and the broader AO network.

This device is essential for:

## Core Functions (Keys)

### `info`

Retrieves or modifies the node's configuration message (often referred to as `NodeMsg` internally).

*   **`GET /~meta@1.0/info`**
    *   **Action:** Returns the current node configuration message.
    *   **Response:** A message map containing the node's settings. Sensitive keys (like private wallets) are filtered out. Dynamically generated keys like the node's public `address` are added if a wallet is configured.
*   **`POST /~meta@1.0/info`**
    *   **Action:** Updates the node's configuration message. The configured `admin` must sign `type: node-message` and the configuration fields to apply. Unsigned fields are ignored, including extensions of signed submessages.
    *   **Request Body:** A message map containing `type: node-message` and the configuration keys and values to update.
    *   **Response:** Confirmation message indicating success or failure.
    *   **Note:** Once a node's configuration is marked as `initialized = permanent`, it cannot be changed via this method.

## Key Configuration Parameters Managed by `~meta`

While the `info` key is the primary interaction point, the `NodeMsg` managed by `~meta` holds crucial configuration parameters affecting the entire node's behavior, including (but not limited to):

*   `port`: HTTP server port.
*   `priv_wallet` / `key_location`: Path to the node's Arweave key file.
*   `operator`: The address designated as the node operator (defaults to the address derived from `priv_wallet`).
*   `admin`: The address allowed to update the node configuration (defaults to `operator`). Both roles accept an address or a list of addresses; `unclaimed` denies access.
*   `initialized`: Status indicating if the node setup is temporary or permanent.
*   `preprocessor` / `postprocessor`: Optional messages defining pre/post-processing logic for requests.
*   `routes`: Routing table used by [`dev_router`](../resources/source-code/dev_router.md).
*   `store`: Configuration for data storage.
*   `trace`: Debug tracing options.
*   `p4_*`: Payment configuration.
*   `faff_*`: Access control lists.

*(Refer to `hb_opts.erl` for a comprehensive list of options.)*

## Utility Functions (Internal/Module Level)

The [`dev_meta.erl`](../resources/source-code/dev_meta.md) module also contains helper functions used internally or callable from other Erlang modules:

*   `is_operator(<RequestMsg>, <NodeMsg>) -> boolean()`: Checks if the signer of `RequestMsg` matches the configured `operator` in `NodeMsg`.
*   `lib_meta:is_authorized(Type, Role, Msg, Opts)`: Verifies the role's signatures, requires their committed `type` to equal `Type`, and returns `{ok, OnlySignedKeysByRole}` or `{error, not_authorized}`. HTTP authorization uses the original singleton before path parsing. Authorization failures return HTTP 403, with a friendly page for browser requests.

Cache writes require the `operator` role and the signed type `cache-write`. Their body must be signed. Batch writes use `write-type: batch`.

## Pre/Post-Processing Hooks

The `~meta` device applies the node's configured `preprocessor` message before resolving the main request and the `postprocessor` message after obtaining the result, allowing for global interception and modification of requests/responses.

## Initialization

Before a node can process general requests, it usually needs to be initialized. Attempts to access devices other than `~meta@1.0/info` before initialization typically result in an error. Initialization often involves setting essential parameters like the operator key via a `POST` to `info`.

[meta module](../resources/source-code/dev_meta.md)
