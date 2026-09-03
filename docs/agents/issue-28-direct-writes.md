# Issue 28 direct-write inventory

Issue 28 moves decoded playback and native-queue ownership writes behind the
runtime spine. Queue workers now emit typed, acked facts for pending slots,
decoded completion, Spotify queue publication/clear, and queue teardown.
Decoded-backend teardown also uses an ordered claim fact; its ack returns the
handles to stop, so ownership is not mutated inside a query. A fetch completion
is applied only when its `fetch_id` still names the live slot; stale workers are
dropped without status or queue advancement. Queue fetches decode before taking
a short sink-start lease; teardown revokes that lease in the fold, making the
claim/start boundary deterministic without blocking the fold on I/O. Terminal
fetch outcomes are typed as success, empty, failure, cancellation, or
supersession; zero-byte downloads use the explicit empty outcome. Queue
transport intents, native-queue pops, and radio resume seeding/dispatch are all
fold facts; paused Spotify recovery stays logical-only and an explicit resume
performs the current-slot load/play. Observed native playing state still comes
only from native-player facts.

The remaining `App` locks in `src/infra/queue/dispatch.rs` are read snapshots
or test-only compatibility setup. Source browse/catalog mutations in
`src/infra/youtube/dispatch.rs` (playlist marker/file-refresh writes) remain
out of scope for issue 28 and are tracked for issue 29.
