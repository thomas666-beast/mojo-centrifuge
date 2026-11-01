# Mojo::Centrifuge 1.0

A Centrifugo-style real-time messaging server for Mojolicious: the **Centrifugo JSON client
protocol**, so the official `centrifuge-js` SDK connects to it, plus JWT auth, namespaces,
history with offset/epoch recovery, presence with join/leave, token refresh, a server HTTP API
and proxy hooks. Redis for multi-worker deployments, an in-memory engine for tests/dev.

## Layout

```
example.pl                         Mojolicious::Lite app (Redis engine)
ws_client.pl                       live end-to-end check against example.pl
public/demo.html                   chat demo using the official centrifuge-js
lib/Mojo/Centrifuge.pm             hub: registry, publish, history, presence, server API
lib/Mojo/Centrifuge/Client.pm      one connection: protocol state machine
lib/Mojo/Centrifuge/JWT.pm         HS256 JWT (alg none / other algs rejected)
lib/Mojo/Centrifuge/Namespaces.pm  per-namespace options + validation
lib/Mojo/Centrifuge/Engine/Redis.pm, Memory.pm
lib/Mojo/Centrifuge/Proxy/HTTP.pm  turn "POST JSON to my backend" into a hook
t/                                 270 tests, no Mojolicious/Redis needed
```

## Run

```
cpanm --installdeps .              # Mojolicious, Mojo::Redis
redis-server &
carton exec morbo example.pl       # http://127.0.0.1:3000/demo.html (open two tabs)
carton exec perl ws_client.pl      # live checks
```

## Using it

```perl
my $hub = Mojo::Centrifuge->new(
    engine  => Mojo::Centrifuge::Engine::Redis->new(redis => Mojo::Redis->new($url)),
    secret  => $ENV{CENTRIFUGE_SECRET},      # JWT HS256 secret
    api_key => $ENV{CENTRIFUGE_API_KEY},
    log     => app->log,
    namespaces => { default => {...}, namespaces => { chat => {...} } },
);

websocket '/connection/websocket' => sub {
    my $c = shift;
    my $client = $hub->new_client(send => sub { $c->send($_[0]) },
                                  close => sub { $c->finish(@_) });
    $c->on(message => sub { $client->handle_frame($_[1]) });
    $c->on(finish  => sub { $client->closed });
};
```

Browser: `new Centrifuge(url, { getToken })`, then `newSubscription('chat:room1')`.
Connection token claims: `sub` (user id), `exp`, `info` (shown in presence), `channels`
(server-side subscriptions). Subscription token claims: `sub`, `channel`, `exp`, `info`.

## Namespaces

`chat:room1` belongs to namespace `chat`; a channel without `:` uses `default`.
Namespaces do not inherit from `default`. Unknown option names die at startup.

| option | meaning |
|---|---|
| `history_size`, `history_ttl` | keep that many publications for that many seconds (set both) |
| `force_recovery` | clients get missed messages after reconnecting (offset + epoch) |
| `force_positioning` | detect lost messages without replaying them |
| `presence`, `join_leave` | presence set; join/leave pushes |
| `allow_subscribe_for_client` | anyone may subscribe (otherwise: subscription token, user-limited channel, or proxy) |
| `allow_publish_for_client` / `_for_subscriber` | client-side publishing |
| `allow_presence_for_client` / `_for_subscriber`, `allow_history_for_client` / `_for_subscriber` | read access |
| `allow_user_limited_channels` | `inbox:#anna,bob` only for the listed users |
| `subscribe_proxy`, `publish_proxy` | ask the `subscribe` / `publish` hook |

## Server API

`POST /api` with `Authorization: apikey <key>` and `{"method": "...", "params": {...}}`, or
`POST /api/<method>`. Methods: `publish`, `broadcast`, `unsubscribe`, `disconnect`, `presence`,
`presence_stats`, `history`, `history_remove`, `channels`, `info`, `batch`.
Errors are `{"error": {"code", "message"}}` with Centrifugo's codes (102 unknown channel,
103 permission denied, 107 bad request, 108 not available, 112 unrecoverable position, ...).

## Hooks (proxy)

`hooks => { connect | subscribe | publish | rpc | refresh => sub { my ($req, $done) = @_; ... $done->({ result => {...} }) } }`
Answer with `{result}`, `{error => {code, message}}` or `{disconnect => {code, reason}}`.
Hooks may answer later (asynchronously); a hook that never answers times out after 10 s.
`Mojo::Centrifuge::Proxy::HTTP->new(endpoint => ...)->hook` forwards to a backend over HTTP.

## Behavior worth knowing

* Every publication, including your own node's, is delivered via the bus in offset order. With Redis
  the `PUBLISH` happens inside the Lua script that assigns the offset, so nodes cannot see offsets out of order.
* A subscriber that sees a gap or an epoch change is disconnected with 3007 and recovers on reconnect.
* Ping every 25 s (empty `{}`), pong within 10 s or the connection is closed with 3009.
* Connection tokens with `exp` are enforced: not refreshed within 25 s after expiry -> closed with 3003.
* Presence entries carry a 60 s TTL and are refreshed every 20 s by each live node, so a crashed
  node's entries disappear by themselves.

## Tests

```
prove -l -It/lib t/                                   # in-memory engine, deterministic fake clock
TEST_REDIS=redis://127.0.0.1:6379/15 prove -l -It/lib t/06_engine_contract.t   # same checks on Redis
```

## Not implemented (compared to Centrifugo)

Protobuf, SSE/HTTP-streaming/WebTransport/GRPC, server-side `subscribe` API, subscription-token
capabilities, delta compression, history `idempotency_key`, cluster-wide `channels`/`info`
(they report this node only), non-blocking Redis calls, metrics.

## Migrating from 0.19

The wire protocol changed (Centrifugo's JSON protocol; use `centrifuge-js`), the endpoint is
`/connection/websocket`, tokens are standard JWTs, `/publish` became the `/api` methods, and the
hand-written `centrifuge-client.js` is gone.
