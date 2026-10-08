"""Deterministic local reconciliation driver; does not execute or prompt any LLM.

This is intentionally a same-machine prototype. A production deployment must
replace the injected Broker with an authenticated remote client and preserve
outbox acknowledgement semantics.
"""
from __future__ import annotations

from .leader import Leader
from .broker import Broker


class LocalCoordinator:
    def __init__(self, leader: Leader, broker: Broker):
        self.leader = leader
        self.broker = broker

    def tick(self) -> dict:
        """Recover lost handoff acknowledgements before consuming terminal events.

        A failed Broker lookup propagates without changing PREPARED handoffs;
        do not fabricate an absent Job from transport failure.
        """
        handoffs = self.leader.reconcile_handoffs(self.broker)
        count = 0
        for event in self.broker.outbox():
            # Inbox commits first, then outbound message may be marked delivered.
            # Crash between those operations is safe: Inbox deduplicates by id.
            self.leader.ingest_event(event)
            self.broker.mark_delivered(event["event_id"])
            count += 1
        expired = self.leader.requeue_expired()
        return {"handoffs": handoffs, "events_delivered": count, "expired_assignments": expired["requeued"]}
