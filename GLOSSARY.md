# Panoptes

Turns the stream of camera detection events into episodes: one operator-facing unit per thing that happened at a camera, instead of dozens of alerts.

## Language

### Input

**Event**:
One detection reported by an edge device for one camera, identified by its `event_id`.
_Avoid_: detection, message, alert

**Kind**:
What was detected: `person_detected`, `loitering`, `line_crossed` or `tamper`.
_Avoid_: type, class

**Severity**:
How important an event is: `info`, `notice`, `warning`, `critical`, in increasing order. Severities are compared by that rank, never as text.
_Avoid_: level, priority

**Device time**:
The moment of detection by the device's clock (`occurred_at`), and the only time that decides which episode an event belongs to.
_Avoid_: event time, timestamp

**Arrival time**:
When the service received an event, by the service's own clock.
_Avoid_: ingest time, received time

**Duplicate**:
A delivery of an event whose `event_id` is already stored.
_Avoid_: replay, repeat

**Out-of-order event**:
An event that arrives after an event of the same episode key with a later device time.
_Avoid_: delayed event

**Invalid event**:
A message that cannot become an event: not JSON, missing a required field, or with a `kind` or `severity` outside the listed sets.
_Avoid_: bad event, poison message

### Episodes

**Episode key**:
The `site_id`, `camera_id` and `kind` that events must share to belong to the same episode.
_Avoid_: group, track, stream

**Episode**:
A run of events with one episode key in which no gap in device time exceeds the quiet window and the whole run stays shorter than the max duration.
_Avoid_: incident, alert, session

**Chapter**:
One of the consecutive episodes that a long activity is cut into at the max duration.
_Avoid_: segment, continuation

**Quiet window**:
The longest device-time gap allowed between consecutive events of one episode (`EPISODE_QUIET_WINDOW`).

**Max duration**:
The longest device-time span one episode may cover, measured from its first seen (`EPISODE_MAX_DURATION`).

**First seen** and **Last seen**:
The earliest and the latest device time among an episode's events.

**Peak severity**:
The highest severity, by rank, among an episode's events.

### Lifecycle

**Open episode**:
An episode that has not settled yet, so the platform has not been told about it.
_Avoid_: active, pending

**Settle**:
To end an episode: record its close reason and the time it settled, and announce it. The brief's "close" means the same thing.
_Avoid_: close, finish, complete

**Settled episode**:
An episode that has settled. It is never merged with another episode or split, but a late event can still join it.
_Avoid_: closed, finished

**Close reason**:
Why an episode settled: `quiet` when the quiet window passed with no further events, `max_duration` when an event started the next chapter.

**Silence**:
How long the service has received no new event for an episode key, by the service's own clock. Silence, not device time, decides when an open episode settles.
_Avoid_: inactivity, timeout

**Settle grace**:
The short wait before settling an episode that already has a later episode with the same key, so that out-of-order events still in flight land first.

**Announcement**:
The one message on `episode.settled.<site_id>.<camera_id>` that tells the platform an episode settled.
_Avoid_: alert, notification

**Late event**:
An event that arrives after the episode it belongs to has settled.
_Avoid_: stale event, straggler

**Correction**:
A change to a settled episode caused by a late event, announced on `episode.corrected.<site_id>.<camera_id>`.
_Avoid_: update, amendment, re-settle

**Revision**:
How many corrections a settled episode has had: 0 when it settles, plus one per correction.
_Avoid_: version
