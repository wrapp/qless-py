#! /usr/bin/env python

from common import TestQless, FooJob

import time
import math
import unittest

import qless
from qless.exceptions import LostLockException, QlessException


class TestPriority(TestQless):
    # Basically all we need to test:
    # 1) If the job doesn't exist, then attempts to set the priority should
    #   return false. This doesn't really matter for us since we're using the
    #   __setattr__ magic method
    # 2) If the job's in a queue, but not yet popped, we should update its
    #   priority in that queue.
    # 3) If a job's in a queue, but already popped, then we just update the 
    #   job's priority.
    def test_priority(self):
        a = self.queue.put(qless.Job, {'test': 'test_priority'}, priority = 10)
        b = self.queue.put(qless.Job, {'test': 'test_priority'})
        self.assertEqual(self.queue.peek().jid, a)
        job = self.client.jobs[b]
        job.priority = 20
        self.assertEqual(len(self.queue), 2)
        self.assertEqual(self.queue.peek().jid, b)
        job = self.queue.pop()
        self.assertEqual(len(self.queue), 2)
        self.assertEqual(job.jid, b)
        job = self.queue.pop()
        self.assertEqual(len(self.queue), 2)
        self.assertEqual(job.jid, a)
        job.priority = 30
        # Make sure it didn't get doubly-inserted in the queue
        self.assertEqual(len(self.queue), 2)
        self.assertEqual(self.queue.peek(), None)
        self.assertEqual(self.queue.pop(), None)

if __name__ == '__main__':
    unittest.main()
