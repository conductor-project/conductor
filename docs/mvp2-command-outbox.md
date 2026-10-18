# MVP2 manager command delivery

The manager commits each new run assignment and its `start_run` command in the
same SQLite transaction. A committed assignment therefore has a stable run ID,
command ID, worker ID, worker incarnation, and serialized command even if the
manager exits before dispatch begins. Repeating the same idempotency key
returns the recorded assignment and command instead of allocating another run.

The manager dispatches the persisted envelope after committing the transaction.
Network calls happen outside SQLite transactions. The remote transport queues
that exact envelope, preserving the command ID across redelivery. Stop and
resolution commands use the same outbox and reuse an identical command for a
repeated request to the same worker incarnation. A successful worker response
is recorded with the command acknowledgement; if the response or acknowledgement
is lost, a retry uses the same command ID and contents. The worker's persisted
run ID prevents a duplicate `start_run` delivery from creating a second process
attempt. An ambiguous dispatch remains pending and the run stays reserved until
the result is observed or an operator resolves it.

At most 32 unacknowledged commands are retained per worker across its
incarnations. Starts and other non-control commands stop at 24, leaving eight
bounded entries available for stop and resolution commands when start delivery
is saturated. Control commands can use any remaining entry up to the total
limit. When all 32 entries are occupied, new commands fail explicitly; the
manager does not silently discard committed assignments.

The manager persists a run's `stopping` observation and its stop command in one
SQLite transaction. A storage or capacity failure rolls back both writes, so a
failed stop request cannot leave a newly accepted stop intent without a
deliverable command. If an older database already contains a `stopping` run
without its command, the next stop request inserts the command before returning
the durable intent. Repeated stop requests find the existing command by its
contents for that worker incarnation, even when later commands for the same run
were written afterward.

Acknowledged command rows are kept indefinitely. No automatic time-based
compaction is performed because delayed retries must not execute a command a
second time. A future compaction protocol must preserve command ID uniqueness
and prove that no valid retry can refer to a removed row.
