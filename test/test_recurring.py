#! /usr/bin/env python

from common import TestQless, FooJob

import time
import math
import unittest

import qless
from qless.exceptions import LostLockException, QlessException

class TestRecurring(TestQless):
    def test_recur_on(self):
        # In this test, we want to enqueue a job and make sure that
        # we can get some jobs from it in the most basic way. We should
        # get jobs out of the queue every _k_ seconds
        time.freeze()
        self.queue.recur(qless.Job, {'test':'test_recur_on'}, interval=1800)
        self.assertEqual(self.queue.pop().complete(), 'complete')
        self.assertEqual(self.queue.pop(), None)
        time.advance(1799)
        self.assertEqual(self.queue.pop(), None)
        time.advance(2)
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        self.assertEqual(job.data, {'test':'test_recur_on'})
        job.complete()
        # We should not be able to pop a second job
        self.assertEqual(self.queue.pop(), None)
        # Let's advance almost to the next one, and then check again
        time.advance(1798)
        self.assertEqual(self.queue.pop(), None)
        time.advance(2)
        self.assertNotEqual(self.queue.pop(), None)
        time.thaw()

    def test_recur_on_by_string(self):
        time.freeze()
        self.queue.recur('qless.Job', {'test':'test_recur_on'}, interval=1800)
        self.assertEqual(self.queue.pop().complete(), 'complete')
        self.assertEqual(self.queue.pop(), None)
        time.advance(1799)
        self.assertEqual(self.queue.pop(), None)
        time.advance(2)
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        self.assertEqual(job.data, {'test':'test_recur_on'})
        job.complete()
        # We should not be able to pop a second job
        self.assertEqual(self.queue.pop(), None)
        # Let's advance almost to the next one, and then check again
        time.advance(1798)
        self.assertEqual(self.queue.pop(), None)
        time.advance(2)
        self.assertNotEqual(self.queue.pop(), None)
        time.thaw()

    def test_recur_attributes(self):
        # Popped jobs should have the same priority, tags, etc. that the
        # recurring job has
        time.freeze()
        self.queue.recur(qless.Job, {'test':'test_recur_attributes'}, interval=100, priority=-10, tags=['foo', 'bar'], retries=2)
        self.assertEqual(self.queue.pop().complete(), 'complete')
        for i in range(10):
            time.advance(100)
            job = self.queue.pop()
            self.assertNotEqual(job, None)
            self.assertEqual(job.priority, -10)
            self.assertEqual(job.tags, ['foo', 'bar'])
            self.assertEqual(job.original_retries, 2)
            self.assertIn(   job.jid, self.client.jobs.tagged('foo')['jobs'])
            self.assertIn(   job.jid, self.client.jobs.tagged('bar')['jobs'])
            self.assertNotIn(job.jid, self.client.jobs.tagged('hey')['jobs'])
            job.complete()
            self.assertEqual(self.queue.pop(), None)
        time.thaw()

    def test_recur_offset(self):
        # In this test, we should get a job after offset and interval
        # have passed
        time.freeze()
        self.queue.recur(qless.Job, {'test':'test_recur_offset'}, interval=100, offset=50)
        self.assertEqual(self.queue.pop(), None)
        time.advance(30)
        self.assertEqual(self.queue.pop(), None)
        time.advance(20)
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        job.complete()
        # And henceforth we should have jobs periodically at 100 seconds
        time.advance(99)
        self.assertEqual(self.queue.pop(), None)
        time.advance(2)
        self.assertNotEqual(self.queue.pop(), None)

    def test_recur_off(self):
        # In this test, we want to make sure that we can stop recurring
        # jobs
        # We should see these recurring jobs crop up under queues when 
        # we request them
        time.freeze()
        jid = self.queue.recur(qless.Job, {'test':'test_recur_off'}, interval=100)
        self.assertEqual(self.queue.pop().complete(), 'complete')
        self.assertEqual(self.client.queues['testing'].counts['recurring'], 1)
        self.assertEqual(self.client.queues.counts[0]['recurring'], 1)
        # Now, let's pop off a job, and then cancel the thing
        time.advance(110)
        self.assertEqual(self.queue.pop().complete(), 'complete')
        job = self.client.jobs[jid]
        self.assertEqual(job.__class__, qless.RecurringJob)
        job.cancel()
        self.assertEqual(self.client.queues['testing'].counts['recurring'], 0)
        self.assertEqual(self.client.queues.counts[0]['recurring'], 0)
        time.advance(1000)
        self.assertEqual(self.queue.pop(), None)

    def test_jobs_recur(self):
        # We should be able to list the jids of all the recurring jobs
        # in a queue
        jids = [self.queue.recur(qless.Job, {'test':'test_jobs_recur'}, interval=i * 10) for i in range(1, 10)]
        self.assertEqual(self.queue.jobs.recurring(), jids)
        for jid in jids:
            self.assertEqual(self.client.jobs[jid].__class__, qless.RecurringJob)

    def test_recur_get(self):
        # We should be able to get the data for a recurring job
        time.freeze()
        jid = self.queue.recur(qless.Job, {'test':'test_recur_get'}, interval=100, priority=-10, tags=['foo', 'bar'], retries=2)
        job = self.client.jobs[jid]
        self.assertEqual(job.__class__ , qless.RecurringJob)
        self.assertEqual(job.priority  , -10)
        self.assertEqual(job.queue_name, 'testing')
        self.assertEqual(job.data      , {'test':'test_recur_get'})
        self.assertEqual(job.tags      , ['foo', 'bar'])
        self.assertEqual(job.interval  , 100)
        self.assertEqual(job.retries   , 2)
        self.assertEqual(job.count     , 0)
        self.assertEqual(job.klass_name, 'qless.job.Job')

        # Now let's pop a job
        self.queue.pop()
        self.assertEqual(self.client.jobs[jid].count, 1)

    def test_passed_interval(self):
        # We should get multiple jobs if we've passed the interval time
        # several times.
        time.freeze()
        jid = self.queue.recur(qless.Job, {'test':'test_passed_interval'}, interval=100)
        self.assertEqual(self.queue.pop().complete(), 'complete')
        time.advance(850)
        jobs = self.queue.pop(100)
        self.assertEqual(len(jobs), 8)
        for job in jobs:
            job.complete()

        # If we are popping fewer jobs than the number of jobs that would have
        # been scheduled, it should only make that many available
        time.advance(800)
        jobs = self.queue.pop(5)
        self.assertEqual(len(jobs), 5)
        self.assertEqual(len(self.queue), 5)
        for job in jobs:
            job.complete()

        # Even if there are several recurring jobs, both of which need jobs
        # scheduled, it only pops off the needed number
        jid = self.queue.recur(qless.Job, {'test': 'test_passed_interval_2'}, 10)
        time.advance(500)
        jobs = self.queue.pop(5)
        self.assertEqual(len(jobs), 5)
        self.assertEqual(len(self.queue), 5)
        for job in jobs:
            job.complete()

        # And if there are other jobs that are there, it should only move over
        # as many recurring jobs as needed
        jid = self.queue.put(qless.Job, {'foo': 'bar'}, priority = 10)
        jobs = self.queue.pop(5)
        self.assertEqual(len(jobs), 5)
        # Not sure why this is 6, but it's not a huge deal in my opinion
        self.assertEqual(len(self.queue), 6)

    def test_queues_endpoint(self):
        # We should see these recurring jobs crop up under queues when 
        # we request them
        jid = self.queue.recur(qless.Job, {'test':'test_queues_endpoint'}, interval=100)
        self.assertEqual(self.client.queues['testing'].counts['recurring'], 1)
        self.assertEqual(self.client.queues.counts[0]['recurring'], 1)

    def test_change_attributes(self):
        # We should be able to change the attributes of a recurring job,
        # and future spawned jobs should be affected appropriately. In
        # addition, when we change the interval, the effect should be 
        # immediate (evaluated from the last time it was run)
        time.freeze()
        jid = self.queue.recur(qless.Job, {'test':'test_change_attributes'}, interval=1)
        self.assertEqual(self.queue.pop().complete(), 'complete')
        job = self.client.jobs[jid]

        # First, test priority
        time.advance(1)
        self.assertNotEqual(self.queue.pop().priority         , -10)
        self.assertNotEqual(self.client.jobs[jid].priority, -10)
        job.priority = -10
        time.advance(1)
        self.assertEqual(   self.queue.pop().priority         , -10)
        self.assertEqual(   self.client.jobs[jid].priority, -10)

        # And data
        time.advance(1)
        self.assertNotEqual(self.queue.pop().data             , {'foo': 'bar'})
        self.assertNotEqual(self.client.jobs[jid].data    , {'foo': 'bar'})
        job.data     = {'foo': 'bar'}
        time.advance(1)
        self.assertEqual(   self.queue.pop().data             , {'foo': 'bar'})
        self.assertEqual(   self.client.jobs[jid].data    , {'foo': 'bar'})

        # And retries
        time.advance(1)
        self.assertNotEqual(self.queue.pop().original_retries , 10)
        self.assertNotEqual(self.client.jobs[jid].retries , 10)
        job.retries  = 10
        time.advance(1)
        self.assertEqual(   self.queue.pop().original_retries , 10)
        self.assertEqual(   self.client.jobs[jid].retries , 10)

        # And klass
        time.advance(1)
        self.assertNotEqual(self.queue.peek().klass_name        , 'qless.job.RecurringJob')
        self.assertNotEqual(self.queue.pop().klass              , qless.RecurringJob)
        self.assertNotEqual(self.client.jobs[jid].klass_name, 'qless.job.RecurringJob')
        self.assertNotEqual(self.client.jobs[jid].klass     , qless.RecurringJob)
        job.klass    = qless.RecurringJob
        time.advance(1)
        self.assertEqual(   self.queue.peek().klass_name        , 'qless.job.RecurringJob')
        self.assertEqual(   self.queue.pop().klass              , qless.RecurringJob)
        self.assertEqual(   self.client.jobs[jid].klass_name, 'qless.job.RecurringJob')
        self.assertEqual(   self.client.jobs[jid].klass     , qless.RecurringJob)

    def test_change_interval(self):
        # If we update a recurring job's interval, then we should get
        # jobs from it as if it had been scheduled this way from the
        # last time it had a job popped
        time.freeze()
        jid = self.queue.recur(qless.Job, {'test':'test_change_interval'}, interval=100)
        self.assertEqual(self.queue.pop().complete(), 'complete')
        time.advance(100)
        self.assertEqual(self.queue.pop().complete(), 'complete')
        time.advance(50)
        # Now, let's update the interval to make it more frequent
        self.client.jobs[jid].interval = 10
        jobs = self.queue.pop(100)
        self.assertEqual(len(jobs), 5)
        results = [job.complete() for job in jobs]
        # Now let's make the interval much longer
        time.advance(49) ; self.client.jobs[jid].interval = 1000; self.assertEqual(self.queue.pop(), None)
        time.advance(100); self.client.jobs[jid].interval = 1000; self.assertEqual(self.queue.pop(), None)
        time.advance(849); self.client.jobs[jid].interval = 1000; self.assertEqual(self.queue.pop(), None)
        time.advance(1)  ; self.client.jobs[jid].interval = 1000; self.assertEqual(self.queue.pop(), None)

    def test_move(self):
        # If we move a recurring job from one queue to another, then
        # all future spawned jobs should be popped from that queue
        time.freeze()
        jid = self.queue.recur(qless.Job, {'test':'test_move'}, interval = 100)
        self.assertEqual(self.queue.pop().complete(), 'complete')
        time.advance(110)
        self.assertEqual(self.queue.pop().complete(), 'complete')
        self.assertEqual(self.other.pop(), None)
        # Now let's move it to another queue
        self.client.jobs[jid].move('other')
        self.assertEqual(self.queue.pop()    , None)
        self.assertEqual(self.other.pop(), None)
        time.advance(100)
        self.assertEqual(self.queue.pop()    , None)
        self.assertEqual(self.other.pop().complete(), 'complete')
        self.assertEqual(self.client.jobs[jid].queue_name, 'other')

    def test_change_tags(self):
        # We should be able to add and remove tags from a recurring job,
        # and see the impact in all the jobs it subsequently spawns
        time.freeze()
        jid = self.queue.recur(qless.Job, {'test':'test_change_tags'}, tags = ['foo', 'bar'], interval = 1)
        self.assertEqual(self.queue.pop().complete(), 'complete')
        time.advance(1)
        self.assertEqual(self.queue.pop().tags, ['foo', 'bar'])
        # Now let's untag the job
        self.client.jobs[jid].untag('foo')
        self.assertEqual(self.client.jobs[jid].tags, ['bar'])
        time.advance(1)
        self.assertEqual(self.queue.pop().tags, ['bar'])

        # Now let's add 'foo' back in, and also add 'hey'
        self.client.jobs[jid].tag('foo', 'hey')
        self.assertEqual(self.client.jobs[jid].tags, ['bar', 'foo', 'hey'])
        time.advance(1)
        self.assertEqual(self.queue.pop().tags, ['bar', 'foo', 'hey'])

    def test_peek(self):
        # When we peek at jobs in a queue, it should take recurring jobs
        # into account
        time.freeze()
        jid = self.queue.recur(qless.Job, {'test':'test_peek'}, interval = 100)
        self.assertEqual(self.queue.pop().complete(), 'complete')
        self.assertEqual(self.queue.peek(), None)
        time.advance(110)
        self.assertNotEqual(self.queue.peek(), None)
        self.queue.pop().complete()

        # If we are popping fewer jobs than the number of jobs that would have
        # been scheduled, it should only make that many available
        time.advance(800)
        jobs = self.queue.peek(5)
        self.assertEqual(len(jobs), 5)
        self.assertEqual(len(self.queue), 5)
        for job in self.queue.pop(100):
            job.complete()
        self.assertEqual(len(self.queue), 0)

        # Even if there are several recurring jobs, both of which need jobs
        # scheduled, it only pops off the needed number
        jid = self.queue.recur(qless.Job, {'test': 'test_peek'}, interval = 10)
        time.advance(800)
        jobs = self.queue.peek(5)
        self.assertEqual(len(jobs), 5)
        self.assertEqual(len(self.queue), 5)
        for job in self.queue.pop(100):
            job.complete()
        self.assertEqual(len(self.queue), 0)

        # And if there are other jobs that are there, it should only move over
        # as many recurring jobs as needed
        time.advance(800)
        jid = self.queue.put(qless.Job, {'foo': 'bar'}, priority=10)
        jobs = self.queue.peek(5)
        self.assertEqual(len(jobs), 5)
        # Not sure why this is 6, but it's not a huge deal in my opinion
        self.assertEqual(len(self.queue), 6)

    def test_jid_counter(self):
        # When we re-recur a job, it should not reset the jid counter
        jid = self.queue.recur(qless.Job, {'test': 'test_jid_counter'}, 10,
            jid='my_recurring_job')
        job = self.queue.pop()
        time.freeze()
        jid = self.queue.recur(qless.Job, {'test': 'test_jid_counter'}, 10,
            jid='my_recurring_job')
        time.advance(15)
        self.assertNotEqual(self.queue.pop().jid, job.jid)

    def test_idempotent_recur(self):
        # When we re-recur a job, it should update the attributes
        jid = self.queue.recur(qless.Job, {'test': 'test_idempotent_recur'}, 10,
            jid='my_recurring_job', priority=10, tags=['foo'], retries=5)
        # Now, let's /re-recur/ the thing, and make sure that its properties
        # have indeed updated as expected
        jid = self.queue.recur(qless.Job, {'test': 'test_idempotent_recur_2'}, 20,
            jid='my_recurring_job', priority=20, tags=['bar'], retries=10)
        job = self.client.jobs[jid]
        self.assertEqual(job.data['test'], 'test_idempotent_recur_2')
        self.assertEqual(job.interval, 20)
        self.assertEqual(job.priority, 20)
        self.assertEqual(job.retries, 10)
        self.assertEqual(job.tags, ['bar'])

    def test_reput(self):
        # If a recurring job exits, and you try to put it into the queue again,
        # then it should behave as a 'move'
        jid = self.queue.recur(qless.Job, {'test': 'test_recur_update'}, 10,
            jid='my_recurring_job')
        counts = self.client.queues.counts
        stats = [c for c in counts if c['name'] == self.queue.name]
        self.assertEqual(len(stats), 1)
        self.assertEqual(stats[0]['recurring'], 1)

        # And we'll reput it into another queue
        jid = self.other.recur(qless.Job, {'test': 'test_recur_update'}, 10,
            jid='my_recurring_job')
        counts = self.client.queues.counts
        stats = [c for c in counts if c['name'] == self.queue.name]
        self.assertEqual(len(stats), 1)
        self.assertEqual(stats[0]['recurring'], 0)
        # Make sure it's in the queue
        counts = self.client.queues.counts
        stats = [c for c in counts if c['name'] == self.other.name]
        self.assertEqual(len(stats), 1)
        self.assertEqual(stats[0]['recurring'], 1)

if __name__ == '__main__':
    unittest.main()
