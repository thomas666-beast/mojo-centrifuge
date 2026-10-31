# Mojo::Centrifuge

Centrifugo-like realtime pub/sub for Mojolicious.

## Status

Early. Wire protocol compatible with Centrifugo's JSON protocol is
being added in Phase 1 (`Mojo::Centrifuge::Protocol`). Until then, the
server speaks a small legacy JSON envelope.

## What it does

- HMAC-SHA256 client tokens with per-channel glob permissions
  (`subscribe`, `publish`, `channels`, legacy `channel`)
- WebSocket endpoint with pre-upgrade authentication (real HTTP 401
  before the upgrade)
- Subscribe / unsubscribe, per-channel history with `since` replay
- Presence (join/leave events + snapshot)
- Keepalive ping/pong with timeout
- Multi-node fan-out via Redis pub/sub
- Backend publishing via `X-API-Key` (or `Authorization: Bearer <api-key>`)
- Client publishing over WS with the same permission rules

## Requirements

- Perl 5.16+
- Mojolicious 9.00+
- Mojo::Redis 3.00+
- Redis 5.0+ (pub/sub; Streams fan-out lands in Phase 2)

## Quick start

```sh
carton install
docker compose up -d redis
CENTRIFUGE_SECRET=s3cr3t CENTRIFUGE_API_KEY=dev \
    carton exec -- perl -Ilib example.pl daemon -l http://*:3000
```

Then mint a token:

```sh
CENTRIFUGE_SECRET=s3cr3t carton exec -- perl make_token.pl alice news 3600
```

And connect:

```text
ws://127.0.0.1:3000/ws?channel=news&token=<token>
```

## Development

```sh
# unit tests (no Redis needed)
carton exec -- prove -l t/

# author tests (POD coverage)
AUTHOR_TESTING=1 carton exec -- prove -l xt/

# build the CPAN tarball
perl Makefile.PL && make manifest && make dist
```

## License
Same as Perl itself. See LICENSE.