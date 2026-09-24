# Dual-MQTT Reporting — Plain-Language Overview

> This document uses placeholders only (`<node-id>`, `<primary-broker>`,
> `<secondary-broker>`). No real hosts, ids, or credentials appear here.

## The one-sentence version

Mesh messages are posted to **two message boards** at once: the private/home
broker and the public community broker — and all the posting is done by the
mux program on the PC, so nothing on the radio node had to change.

## The problem we had

Each mesh radio can send its messages to the internet (MQTT) by itself, but
ours had gone quiet on the public board — new messages simply never showed
up there (checked with an independent map service: zero sightings for fresh
messages). The home board kept working because the mux was posting there. So
the mesh was effectively invisible to the outside world.

## The fix, in everyday terms

Think of MQTT brokers as **mailboxes where mesh messages get pinned up**:

* **Mailbox 1 (home):** the broker on the local network.
* **Mailbox 2 (public):** the community broker that feeds public maps.

The mux program — the bit that sits between the radio and the phone apps —
now puts a copy of every message into **both** mailboxes. It signs both
copies with one shared name tag per boot (`mux-v{ver}-{mqtt_username}-{nodehex}`
or `mux-v{ver}-{nodehex}-{uuid12}`): both legs use the same id by design, which
is safe only because the mux disables the second leg when both brokers point at
the same address. The per-boot random part keeps it from colliding with the node
itself or anyone's phone. Note: the public broker has in the past refused
computer sessions with "not authorized" errors — if the public side
stops working, the fallback is a one-line
config change (see "What to keep an eye on").

What goes where:

* Normal channel messages → both mailboxes (the `msh/...` notice board).
* Technical raw copies → at this site, home mailbox only (keeps the public board clean).
  The code supports per-leg `raw_uplink` + `raw_scope` on both legs (`uncovered`
  skips proxy-covered ids, `all` takes everything); this site leaves raw off on public.
* Nothing is *taken* from either mailbox back into the radio at this site —
  the fetch switch (`downlink_enabled`) is off, so the mux only posts outward
  (this also removes any risk of message loops). Downlink exists as a config
  option but is disabled here.

## What changed, concretely

* A small software update to the mux (it can now hold two mailbox logins and
  post each message twice).
* Two settings flips: no fetching messages *from* the mailboxes, and the
  second mailbox login (username/password were read from the node's own
  settings and pasted into the private site config — the mux itself never
  writes credentials or server addresses to the node; they live only in a
  private, gitignored config file, never published).
* The radio node itself: **untouched**. It keeps doing exactly what it did.

## How we know it works

* The mux's own status line counts both mailboxes separately — both counters
  climb together.
* An independent check: fresh message IDs from the node show up on the
  public board's map feed with a "came in over the internet, not over radio"
  signature. Before the fix, the same check came back empty repeatedly.

## What to keep an eye on

* **Posting stops on the public side?** The mux logs will show repeated
  "secondary connection lost" lines — tell the admin. If they say "not
  authorized", the broker is rejecting the mux name tag: switch the second
  mailbox off again (one-line change plus restart) — there is no alternate
  identity to fall back to by design.
* **Both mailboxes accidentally set to the same address?** The mux notices
  and posts only once (the second copy switches itself off with a
  "duplicates primary" warning) — no duplicate posts, no session fights.
* **Radio/battery impact?** None. The mux only *reads* what the radio already
  sends; all internet posting happens on the PC.
* **Privacy?** Messages already marked "OK to MQTT" on the mesh are exactly
  what gets posted — same as the node would publish itself. The raw technical
  copies never leave the home network.
* **If the node starts posting to the public board by itself again?** Each
  message would appear twice there (once from the node, once from the mux).
  If the broker starts kicking sessions over it, turn one of the two off —
  that's the known fallback.
