# External HTTP side effects

MonsterFt runs `apply` on multiple nodes and requires agreement on results. An
HTTP Server must therefore give every member node the same stable response.
The minimal pattern uses two commands:

1. POST the same fact from every member under one `requestId`.
2. GET the fact under one `observationId` after a quorum has reported it.

[example4.js](example4.js) is the complete runnable implementation.

## Protocol

POST `/facts` puts the member node ID in a header:

```text
MonsterFt-Node-Id: 1
```

The body contains only values shared by every node:

```js
{ requestId: 'a1b2c3', fact: { account: 'alice', balance: 42 } }
```

The HTTP Server hashes the fact and stores one immutable `{ hash, fact }`
report per `(requestId, nodeId)`. Repeating the same hash is idempotent; a
different hash for that key is a conflict. Example 4 hashes `JSON.stringify(fact)`
with SHA-256. Use a canonical serialization if fact property order can vary.

The response omits `nodeId` so matching reports produce the same result:

```js
{
  status: 'reported',
  requestId: 'a1b2c3',
  fact: { account: 'alice', balance: 42 },
}
```

GET `/facts?requestId=a1b2c3&observationId=d4e5f6` groups reports by hash. A
matching quorum returns the stored fact for the winning hash:

```js
{
  status: 'confirmed',
  requestId: 'a1b2c3',
  observationId: 'd4e5f6',
  fact: { account: 'alice', balance: 42 },
}
```

Without a quorum, the status is `pending` and `fact` is omitted. The first
result for `(requestId, observationId)` is frozen, so every node evaluating
that GET command receives the same response even if more reports arrive.

Example 4 stores reports and observations in memory. A real remote must
store them durably.

## Apply callback

```js
const apply = async (db, buf, term, seq) => {
  if (seq === 0n) { return }
  const command = JSON.parse(buf.toString('utf8'))

  if (command.method === 'POST') {
    return requestJson(factUrl, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'MonsterFt-Node-Id': id,
      },
      body: JSON.stringify({
        requestId: command.requestId,
        fact: command.fact,
      }),
    })
  }

  if (command.method === 'GET') {
    const url = new URL(factUrl)
    url.searchParams.set('requestId', command.requestId)
    url.searchParams.set('observationId', command.observationId)
    return requestJson(url)
  }

  throw new Error(`unknown method: ${command.method}`)
}
```

## Caller pattern

```js
const requestId = randomUUID()
const fact = { account: 'alice', balance: 42 }
await nodes[0].append(toBuf({ method: 'POST', requestId, fact }))

const observationId = randomUUID()
const [getSeq, getResult] = await nodes[1].append(toBuf({
  method: 'GET', requestId, observationId,
}))

if (getResult.status !== 'confirmed') {
  throw new Error('the remote server did not confirm the fact')
}
```

The POST command cannot succeed until MonsterFt receives matching results
from a quorum. Because the MonsterFt cluster and HTTP server agree on
what defines a quorum the GET should immediately return the confirmed
fact; no retry loop needed.

## Copyright
LOCK HOST, INC. 2026. GNU AGPLv3.

Commercial license contact: hello@lock.host.
