#! /usr/bin/env python

from common import TestQless

import time
import unittest

# Qless dependencies
import qless
from qless.exceptions import QlessException


class TestEvents(TestQless):
    '''We'd like to make sure that we can get any events that are emitted'''
    def setUp(self):
        TestQless.setUp(self)
        import threading
        from collections import defaultdict
        self.tracked   = self.client.jobs[self.queue.put(qless.Job, {'tracked': True })]
        self.untracked = self.client.jobs[self.queue.put(qless.Job, {'tracked': False})]
        self.tracked.track()
        self.events    = defaultdict(set)
        self.pubsub    = qless.client(socket_timeout=0.01)
        # We need to make sure this gets lazily loaded first in this thread
        self.pubsub.events
        self.t         = threading.Thread(target=self._listen)

    def tearDown(self):
        TestQless.tearDown(self)

    def _events(self, evt, jid):
        self.events[evt].add(jid)

    def _listen(self):
        from functools import partial
        for evt in ('canceled', 'completed', 'failed', 'popped', 'put', 'stalled', 'track', 'untrack'):
            self.pubsub.events.on(evt, partial(self._events, evt))
        self.pubsub.events.listen()

    def test_cancel(self):
        # We should be able to see when tracked jobs are canceled
        self.t.start()
        self.tracked.cancel()
        self.untracked.cancel()
        self.t.join()
        self.assertEqual(self.events['canceled'], set([self.tracked.jid]))

    def test_complete(self):
        # And when they've completed
        self.t.start()
        # Complete both jobs
        r = [j.complete() for j in self.queue.pop(2)]
        self.t.join()
        self.assertEqual(self.events['completed'], set([self.tracked.jid]))

    def test_failed(self):
        # And when they've completed
        self.t.start()
        # Fail both jobs
        r = [j.fail('foo', 'bar') for j in self.queue.pop(2)]
        self.t.join()
        self.assertEqual(self.events['failed'], set([self.tracked.jid]))

    def test_pop(self):
        # And when they've completed
        self.t.start()
        # Pop both jobs
        jobs = self.queue.pop(2)
        self.t.join()
        self.assertEqual(self.events['popped'], set([self.tracked.jid]))

    def test_put(self):
        # And when they've completed
        self.t.start()
        # Move both jobs
        self.untracked.move('other')
        self.tracked.move('other')
        self.t.join()
        self.assertEqual(self.events['put'], set([self.tracked.jid]))

    def test_stalled(self):
        # And when they've completed
        self.t.start()
        # Stall both jobs
        time.freeze()
        jobs = self.queue.pop(2)
        time.advance(600)
        jobs = self.queue.pop(2)
        # Make sure they in fact stalled
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0].original_retries - jobs[0].retries_left, 1)
        self.assertEqual(jobs[1].original_retries - jobs[1].retries_left, 1)
        self.t.join()
        self.assertEqual(self.events['stalled'], set([self.tracked.jid]))

    def test_track(self):
        # And when they've completed
        self.t.start()
        # Start tracking the untracked job, and untrack the tracked job
        self.tracked.untrack()
        self.untracked.track()
        self.t.join()
        self.assertEqual(self.events['track'], set([self.untracked.jid]))
        self.assertEqual(self.events['untrack'], set([self.tracked.jid]))

if __name__ == '__main__':
    unittest.main()
