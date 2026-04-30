# Streaming path strips exception cause chains

## What we see on the client
```
stream_exception: Query Failed [Failed to execute main query]
```
No stacktrace, no cause, no hint at what actually blew up inside the shard.

## Why
Trace from shard to coord on a streaming query failure:

1. Query phase throws → `QueryPhase.java:307` wraps the underlying exception in
   `new QueryPhaseExecutionException(shardTarget, "Failed to execute main query", e)` — message becomes
   `"Query Failed [Failed to execute main query]"` per `QueryPhaseExecutionException.java:51`.
2. `StreamSearchChannelListener.java:68-75` catches and calls `channel.sendResponse(e)`.
3. `FlightTransportChannel.java:58-83` → `FlightOutboundHandler.sendErrorResponse(..., exception)` →
   `FlightOutboundHandler.java:265-283 processErrorTask`. Since `QueryPhaseExecutionException` is **not** a
   `StreamException`, the `if (task.error() instanceof StreamException se)` branch at line 275 is skipped —
   exception passes through to `flightServerChannel.sendError`.
4. `FlightServerChannel.java:169-194 sendError`:
   ```java
   flightExc = CallStatus.INTERNAL.withCause(error)
       .withDescription(error.getMessage() != null ? error.getMessage() : "Stream error")
       .toRuntimeException();
   ```
   The Arrow `CallStatus` carries only a **description string** over the wire. `withCause(error)` attaches the
   `Throwable` object locally for Flight's own diagnostics, but gRPC doesn't serialize arbitrary Java
   `Throwable` objects — only the description string survives the hop.
5. On the client, `FlightErrorMapper.fromFlightException` (lines 63–71) rebuilds a `StreamException` using
   `exception.getMessage()` — which is just the description, i.e. the outer wrapper's message. The original
   cause is lost.

## Impact
- When streaming fails in `QueryPhase.executeInternal`, there's no way from the client to tell if the cause
  was a CB trip, a NPE in a streaming aggregator, an OOM, a bug in the planner, or anything else.
- Data-node logs have the real stacktrace (`FlightServerChannel.java:186 logger.error("Unexpected error in
  Flight stream", error)` logs it), but that requires tumbler / SSH access to the data node.

## Suggested fix (future work, not in current plan)
Extend `FlightServerChannel.sendError` to append a short cause summary to the description — at minimum the
root-cause exception class name and first line. Something like:
```java
String desc = error.getMessage();
Throwable root = org.opensearch.ExceptionsHelper.unwrapCause(error);
if (root != null && root != error) {
    desc = desc + " | cause: " + root.getClass().getSimpleName() + ": " + root.getMessage();
}
```
This is strictly additive — the Flight description field is free-form — and wouldn't require a wire-format
change. Would need to be mindful of message-length limits (gRPC default is ~8KB for status messages).

Alternatively: serialize the full `OpenSearchException` chain into a Flight metadata header and rehydrate on
the client. That's a bigger change and requires the `skipMetadata` flag in `FlightErrorMapper.java:33` to be
flipped, which the TODO there suggests was deliberately off for security review.

## Why this matters for Omnissa right now
The baseline failures I saw (`stream_exception` on the nested high-cardinality queries) might have been:
- The parent CB on the coord (contamination from your sidecar work) — most likely
- A real bug in the streaming reduce path — can't rule out
- A different CB inside the shard — can't rule out

Without the root cause being visible, I can't distinguish these on the current release. When we rerun the
baseline on a quiet cluster, I'll have to pull data-node logs via tumbler during the failing query to
confirm what's actually happening.
