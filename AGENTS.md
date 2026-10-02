# Agents.md

## Known issues

It is understood that SQLite patchsets should not always be hashed as
opaque buffers. SQLite patchsets can be iterated through using SQLite
library functions and it would be more correct to do this. For now it
has been shown that when all cluster members are on the same version
of nodejs the patchset buffers can be simply hashed and so they are.

It is understood that passing a sparse array into appendBatch will
cause problems but we don't really care about this. This nodejs impl
is to serve as a reference impl for learning and some edge cases are
not worth putting into code if they clutter the sources.

It is understood that allowing users to choose the SQLite `:memory:`
persistence layer is sort of antithetical to Raft but we let the user
do what they want here.

It is understood that if Date.now rolls backwards the cluster may
have trouble with elections but there is no risk to data corruption.
A future nodejs and rust impl are planned to cover this case, and to
improve other things related to Raft being a "strong leader" protocol
but MonsterFt wanting things a little different.
