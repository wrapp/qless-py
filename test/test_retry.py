#! /usr/bin/env python

from common import TestQless, FooJob

import time
import math
import unittest

import qless
from qless.exceptions import LostLockException, QlessException


class TestRetry(TestQless):
    # It should decrement retries, and put it back in the queue. If retries
    # have been exhausted, then it should be marked as failed.
    # Prohibitions:
    #   1) We can't retry from another worker
    #   2) We can't retry if it's not running
    def test_retry(self):
        jid = self.queue.put(qless.Job, {'test': 'test_retry'})
        job = self.queue.pop()
        self.assertEqual(job.original_retries, job.retries_left)
        job.retry()
        # Pop it off again
        self.assertEqual(self.queue.jobs.scheduled(), [])
        self.assertEqual(self.client.jobs[job.jid].state, 'waiting')
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        self.assertEqual(job.original_retries, job.retries_left + 1)
        # Retry it again, with a backoff
        job.retry(60)
        self.assertEqual(self.queue.pop(), None)
        self.assertEqual(self.queue.jobs.scheduled(), [jid])
        job = self.client.jobs[jid]
        self.assertEqual(job.original_retries, job.retries_left + 2)
        self.assertEqual(job.state, 'scheduled')

    def test_retry_fail(self):
        # Make sure that if we exhaust a job's retries, that it fails
        jid = self.queue.put(qless.Job, {'test': 'test_retry_fail'}, retries=2)
        self.assertEqual(self.client.jobs.failed(), {})
        self.assertEqual(self.queue.pop().retry(), 1)
        self.assertEqual(self.queue.pop().retry(), 0)
        self.assertEqual(self.queue.pop().retry(), -1)
        self.assertEqual(self.client.jobs.failed(), {
            'failed-retries-testing': 1
        })

    def test_retry_error(self):
        # These are some of the conditions under which we cannot retry a job
        job = self.client.jobs[self.queue.put(qless.Job, {'test': 'test_retry_error'})]
        self.assertRaises(QlessException, job.retry)
        self.queue.pop().fail('foo', 'bar')
        self.assertRaises(QlessException, self.client.jobs[job.jid].retry)
        self.client.jobs[job.jid].move('testing')
        job = self.queue.pop(); job.worker_name = 'foobar'
        self.assertRaises(QlessException, job.retry)
        job.worker_name = self.client.worker_name
        job.complete()
        self.assertRaises(QlessException, job.retry)

    def test_retry_workers(self):
        # When we retry a job, it shouldn't be reported as belonging to that worker
        # any longer
        jid = self.queue.put(qless.Job, {'test': 'test_retry_workers'})
        job = self.queue.pop()
        self.assertEqual(self.client.workers[self.client.worker_name], {'jobs': [jid], 'stalled': []})
        self.assertEqual(job.retry(), 4)
        self.assertEqual(self.client.workers[self.client.worker_name], {'jobs': [], 'stalled': []})

    def test_retry_complete(self):
        # We shouldn't be able to complete a job that has been selected for
        # retry
        jid = self.queue.put(qless.Job, {'test': 'test_retry_complete'})
        job = self.queue.pop()
        job.retry()
        self.assertRaises(QlessException, job.complete)
        self.assertRaises(QlessException, job.retry)
        job = self.client.jobs[jid]
        self.assertEqual(job.state, 'waiting')

if __name__ == '__main__':
    unittest.main()
