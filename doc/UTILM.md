# Monster helpers

Function notes for [src/utilm.js](../src/utilm.js), in source order.
Trivial conversion and type-check helpers are omitted, and nested callbacks
are covered with their enclosing function.

## `outcomesWire`

Converts command outcomes to wire tuples, optionally retaining error stacks.

### findings

no findings

## `outcomesPublic`

Converts wire outcome tuples into public fulfilled and rejected results.

### findings

no findings

## `digestCmd`

Produces the command agreement digest from the command identity, patchset,
and outcomes, excluding error stacks.

### findings

- **Accepted: patchsets are hashed as opaque buffers.** Cluster members must
  use the same Node.js version; interpreting and normalizing patchset contents
  is deferred, as noted in [AGENTS.md](../AGENTS.md).
- **Accepted encoding requirement:** outcomes must encode identically across
  nodes, including object key order. Agreement excludes error stacks.

## `assertEncodable`

Rejects Apply results that cannot be encoded as MessagePack.

### findings

no findings

## `digestEntry`

Produces the checkpoint hash identifying an entry's term and payload.

### findings

no findings

## `validateOutcomeRequestMsg`

Validates the command sequence in an outcome request.

### findings

no findings

## `validateOutcomeMsg`

Validates the command sequence and digest in an outcome message.

### findings

no findings

## `validateCmdEntry`

Validates a CMD entry's replication positions and command items.

### findings

no findings

## `validateNodes`

Validates a SYNC entry's ordered, distinct cluster members and required count.

### findings

no findings

## `validateSyncEntry`

Validates a SYNC decision and returns its agreeing and disagreeing members.

### findings

- **Open: different quorum settings can invalidate a committed SYNC.**
  Leaders decide using their configured `quorum`, while receivers validate the
  agree list against their own value. A leader with a smaller quorum can return
  success before another node rejects its SYNC. Increasing quorum can also
  invalidate historical SYNC records that have not yet been applied. The
  requirement for matching quorum settings across the cluster is not explicit
  in the README; leave this documentation issue open.

## `isFwdCmdMsg`

Checks whether a forwarded command has a valid request identity, term, and
command items.

### findings

no findings

## `isOutcomeTuple`

Checks the format of a successful or rejected command outcome tuple.

### findings

no findings

## `isFwdCmdAck`

Checks a forwarded command acknowledgment's sequence and result list against
the expected number of items.

### findings

no findings

## `normalizeKeepOptions`

Validates retention settings and returns normalized thresholds, or disables
retention when both settings are omitted.

### findings

no findings
