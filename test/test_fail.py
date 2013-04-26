#! /usr/bin/env python

from common import TestQless, FooJob

import time
import math
import unittest

import qless
from qless.exceptions import LostLockException, QlessException


class TestFail(TestQless):
    def test_fail_failed(self):
        # In this test, we want to make sure that we can correctly 
        # fail a job
        #   1) Put a job
        #   2) Fail a job
        #   3) Ensure the queue is empty, and that there's something
        #       in the failed endpoint
        #   4) Ensure that the job still has its original queue
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        self.assertEqual(len(self.client.jobs.failed()), 0)
        jid = self.queue.put(qless.Job, {'test': 'fail_failed'})
        job = self.queue.pop()
        job.fail('foo', 'Some sort of message')
        self.assertEqual(self.queue.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {
            'foo': 1
        })
        results = self.client.jobs.failed('foo')
        self.assertEqual(results['total'], 1)
        job = results['jobs'][0]
        self.assertEqual(job.jid        , jid)
        self.assertEqual(job.queue_name , 'testing')
        self.assertEqual(job.queue.name , 'testing')
        self.assertEqual(job.data       , {'test': 'fail_failed'})
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.state      , 'failed')
        self.assertEqual(job.retries_left , 5)
        self.assertEqual(job.original_retries   , 5)
        self.assertEqual(job.klass_name , 'qless.job.Job')
        self.assertEqual(job.klass      , qless.Job)
        self.assertEqual(job.tags       , [])

    def test_pop_fail(self):
        # In this test, we want to make sure that we can pop a job,
        # fail it, and then we shouldn't be able to complete /or/ 
        # heartbeat the job
        #   1) Put a job
        #   2) Fail a job
        #   3) Heartbeat to job fails
        #   4) Complete job fails
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        self.assertEqual(len(self.client.jobs.failed()), 0)
        jid = self.queue.put(qless.Job, {'test': 'pop_fail'})
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        job.fail('foo', 'Some sort of message')
        self.assertEqual(len(self.queue), 0)
        self.assertRaises(LostLockException, job.heartbeat)
        self.assertRaises(QlessException, job.complete)
        self.assertEqual(self.client.jobs.failed(), {
            'foo': 1
        })
        results = self.client.jobs.failed('foo')
        self.assertEqual(results['total'], 1)
        self.assertEqual(results['jobs'][0].jid, jid)

    def test_fail_state(self):
        # We shouldn't be able to fail a job that's in any state but
        # running
        self.assertEqual(len(self.client.jobs.failed()), 0)
        job = self.client.jobs[self.queue.put(qless.Job, {'test': 'fail_state'})]
        self.assertRaises(QlessException, job.fail, 'foo', 'Some sort of message')
        self.assertEqual(len(self.client.jobs.failed()), 0)
        job = self.client.jobs[self.queue.put(qless.Job, {'test': 'fail_state'}, delay=60)]
        self.assertRaises(QlessException, job.fail, 'foo', 'Some sort of message')
        self.assertEqual(len(self.client.jobs.failed()), 0)        
        jid = self.queue.put(qless.Job, {'test': 'fail_complete'})
        job = self.queue.pop()
        job.complete()
        self.assertRaises(QlessException, job.fail, 'foo', 'Some sort of message')
        self.assertEqual(len(self.client.jobs.failed()), 0)

    def test_put_failed(self):
        # In this test, we want to make sure that if we put a job
        # that has been failed, we want to make sure that it is
        # no longer reported as failed
        #   1) Put a job
        #   2) Fail that job
        #   3) Make sure we get failed stats
        #   4) Put that job on again
        #   5) Make sure that we no longer get failed stats
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        jid = self.queue.put(qless.Job, {'test': 'put_failed'})
        job = self.queue.pop()
        job.fail('foo', 'some message')
        self.assertEqual(self.client.jobs.failed(), {'foo':1})
        job.move('testing')
        self.assertEqual(len(self.queue), 1)
        self.assertEqual(self.client.jobs.failed(), {})

    def test_complete_failed(self):
        # No matter if a job has been failed before or not, then we
        # should delete the failure information we have once a job
        # has completed.
        jid = self.queue.put(qless.Job, {'test': 'put_failed'})
        job = self.queue.pop()
        job.fail('foo', 'some message')
        job.move('testing')
        job = self.queue.pop()
        self.assertEqual(job.complete(), 'complete')
        self.assertEqual(self.client.jobs[jid].failure, {})

    def test_unfail(self):
        # We should be able to unfail jobs in batch. First, fail a large number
        # of jobs, and then unfail some of them. Make sure that those jobs have
        # indeed moved, all the remaining failed jobs are still failed, and
        # that the new jobs can be popped and work like they should
        jids = [self.queue.put(qless.Job, {'test': 'test_unfail'})
            for i in range(1000)]
        self.assertEqual(len(
            [job.fail('foo', 'bar') for job in self.queue.pop(1000)]), 1000)
        self.assertEqual(self.client.unfail('foo', self.queue.name, 25), 25)
        self.assertEqual(self.client.jobs.failed()['foo'], 975)
        failed = self.client.jobs.failed('foo', 0, 1000)['jobs']
        self.assertEqual(set([j.jid for j in failed]), set(jids[25:]))

        # Now make sure that they can be popped off and completed correctly
        self.assertEqual(
            [j.complete() for j in self.queue.pop(1000)],
            ['complete'] * 25)

if __name__ == '__main__':
    unittest.main()
