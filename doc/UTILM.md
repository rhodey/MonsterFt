# Monster helpers

Function notes for [src/utilm.js](../src/utilm.js), in source order.
Trivial conversion and type-check helpers are omitted, and nested callbacks
are covered with their enclosing function.

## `isInputType`

Identifies local message types that require ordered Proc execution.

### findings

no findings

## `isOutcomeTuple`

Checks the format of a successful or rejected command outcome tuple.

### findings

no findings

## `isFwdCmdMsg`

Checks whether a message is a valid forwarded command.

### findings

no findings

## `isFwdCmdAck`

Checks whether a forwarded command acknowledgment is valid for the expected
number of items.

### findings

no findings

## `assertEncodable`

Rejects Apply results that cannot be encoded as MessagePack.

### findings

no findings

## `outcomesWire`

Converts command outcomes to wire tuples, optionally retaining error stacks.

### findings

no findings

## `outcomesPublic`

Converts wire outcome tuples into public fulfilled and rejected results.

### findings

no findings

## `digestEntry`

Produces the checkpoint hash used to identify a log entry.

### findings

no findings

## `digestCmd`

Produces the digest used for command agreement.

### findings

- **Accepted: patchsets are hashed as opaque buffers.** Cluster members must
  use the same Node.js version; normalizing patchset contents is deferred.
- **Accepted encoding requirement:** outcomes must encode identically across
  nodes, including object key order. Agreement excludes error stacks.

## `normalizeKeepOptions`

Validates and normalizes log-retention settings.

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

## `validateCommandResult`

Validates a command result against the number of requested items.

### findings

no findings

## `validateRequest`

Validates local requests sent to Proc or Apply.

### findings

no findings

## `validateExecute`

Validates Apply execution fields for setup or a command.

### findings

no findings

## `validateLocalAckErr`

Validates local completion replies against their original requests and sending
roles.

### findings

no findings

## `parseOutput`

Decodes and validates a Proc output envelope without changing its message.

### findings

no findings

## `decodeEntry`

Decodes a CMD or SYNC log entry.

### findings

no findings

## `validateNodes`

Validates the cluster membership recorded by a SYNC entry.

### findings

no findings

## `validateCmdEntry`

Validates a replicated command entry.

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

## `validateEvent`

Validates a protocol event for the current cluster membership.

### findings

no findings

## `parseProcRequest`

Validates a local Proc request and extracts any embedded cluster message.

### findings

no findings
