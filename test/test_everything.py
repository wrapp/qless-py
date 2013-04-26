#! /usr/bin/env python

from common import TestQless, FooJob

import time
import math
import unittest

# Qless dependencies
from qless import Job
from qless.exceptions import LostLockException, QlessException


class TestEverything(TestQless):
    def test_config(self):
        # Set this particular configuration value
        config = self.client.config
        config['testing'] = 'foo'
        self.assertEqual(config['testing'], 'foo')
        # Now let's get all the configuration options and make
        # sure that it's a dictionary, and that it has a key for 'testing'
        self.assertTrue(isinstance(config.all, dict))
        self.assertEqual(config.all['testing'], 'foo')
        # Now we'll delete this configuration option and make sure that
        # when we try to get it again, it doesn't exist
        del config['testing']
        self.assertEqual(config['testing'], None)
        self.assertRaises(AttributeError, config.__getattr__, 'foo')

    def test_put_get(self):
        # In this test, I want to make sure that I can put a job into
        # a queue, and then retrieve its data
        #   1) put in a job
        #   2) get job
        #   3) delete job
        jid = self.queue.put(Job, {'test': 'put_get'})
        job = self.client.jobs[jid]
        self.assertEqual(job.priority   , 0)
        self.assertEqual(job.data       , {'test': 'put_get'})
        self.assertEqual(job.tags       , [])
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.state      , 'waiting')
        self.assertEqual(job.klass_name , 'qless.job.Job')
        self.assertEqual(job.klass      , Job)
        # Make sure the times for the history match up
        job.history[0]['put'] = math.floor(job.history[0]['put'])
        self.assertEqual(job.history , [{
            'q'    : 'testing',
            'put'  : math.floor(time.time())
        }])

    def test_push_peek_pop_many(self):
        # In this test, we're going to add several jobs, and make
        # sure that they:
        #   1) get put onto the queue
        #   2) we can peek at them
        #   3) we can pop them all off
        #   4) once we've popped them off, we can 
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        jids = [self.queue.put(Job, {'test': 'push_pop_many', 'count': c}) for c in range(10)]
        self.assertEqual(len(self.queue), len(jids), 'Inserting should increase the size of the queue')

        # Alright, they're in the queue. Let's take a peek
        self.assertEqual(len(self.queue.peek(7)) , 7)
        self.assertEqual(len(self.queue.peek(10)), 10)

        # Now let's pop them all off one by one
        self.assertEqual(len(self.queue.pop(7)) , 7)
        self.assertEqual(len(self.queue.pop(10)), 3)

    def test_put_pop_attributes(self):
        # In this test, we want to put a job, pop a job, and make
        # sure that when popped, we get all the attributes back 
        # that we expect
        #   1) put a job
        #   2) pop said job, check existence of attributes
        jid = self.queue.put(Job, {'test': 'test_put_pop_attributes'})
        self.client.config['heartbeat'] = 60
        job = self.queue.pop()
        self.assertEqual(job.data       , {'test': 'test_put_pop_attributes'})
        self.assertEqual(job.worker_name, self.client.worker_name)
        self.assertTrue( job.ttl        > 0)
        self.assertEqual(job.state      , 'running')
        self.assertEqual(job.queue_name , 'testing')
        self.assertEqual(job.queue.name , 'testing')
        self.assertEqual(job.retries_left , 5)
        self.assertEqual(job.original_retries   , 5)
        self.assertEqual(job.jid        , jid)
        self.assertEqual(job.klass_name , 'qless.job.Job')
        self.assertEqual(job.klass      , Job)
        self.assertEqual(job.tags       , [])
        jid = self.queue.put(FooJob, {'test': 'test_put_pop_attributes'})
        job = self.queue.pop()
        self.assertTrue('FooJob' in job.klass_name)

    def test_data_access(self):
        # In this test, we'd like to make sure that all the data attributes
        # of the job can be accessed through __getitem__
        #   1) Insert a job
        #   2) Get a job,  check job['test']
        #   3) Peek a job, check job['test']
        #   4) Pop a job,  check job['test']
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        jid = self.queue.put(Job, {'test': 'data_access'})
        job = self.client.jobs[jid]
        self.assertEqual(job['test'], 'data_access')
        job = self.queue.peek()
        self.assertEqual(job['test'], 'data_access')
        job = self.queue.pop()
        self.assertEqual(job['test'], 'data_access')

    def test_put_pop_priority(self):
        # In this test, we're going to add several jobs and make
        # sure that we get them in an order based on priority
        #   1) Insert 10 jobs into the queue with successively more priority
        #   2) Pop all the jobs, and ensure that with each pop we get the right one
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        jids = [self.queue.put(Job, {'test': 'put_pop_priority', 'count': c}, priority=c) for c in range(10)]
        last = len(jids)
        for i in range(len(jids)):
            job = self.queue.pop()
            self.assertTrue(job['count'] < last, 'We should see jobs in reverse order')
            last = job['count']

    def test_same_priority_order(self):
        # In this test, we want to make sure that jobs are popped
        # off in the same order they were put on, priorities being
        # equal.
        #   1) Put some jobs
        #   2) Pop some jobs, save jids
        #   3) Put more jobs
        #   4) Pop until empty, saving jids
        #   5) Ensure popped jobs are in the same order
        jids   = []
        popped = []
        for count in range(100):
            jids.append(self.queue.put(Job, {'test': 'put_pop_order', 'count': 2 * count }))
            self.queue.peek()
            jids.append(self.queue.put(FooJob, {'test': 'put_pop_order', 'count': 2 * count+ 1 }))
            popped.append(self.queue.pop().jid)
            self.queue.peek()

        popped.extend(self.queue.pop().jid for i in range(100))
        self.assertEqual(jids, popped)

    def test_scheduled(self):
        # In this test, we'd like to make sure that we can't pop
        # off a job scheduled for in the future until it has been
        # considered valid
        #   1) Put a job scheduled for 10s from now
        #   2) Ensure an empty pop
        #   3) 'Wait' 10s
        #   4) Ensure pop contains that job
        # This is /ugly/, but we're going to path the time function so
        # that we can fake out how long these things are waiting
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        time.freeze()
        jid = self.queue.put(Job, {'test': 'scheduled'}, delay=10)
        self.assertEqual(self.queue.pop(), None)
        self.assertEqual(len(self.queue), 1)
        time.advance(11)
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        self.assertEqual(job.jid, jid)
        time.thaw()

    def test_scheduled_peek_pop_state(self):
        # Despite the wordy test name, we want to make sure that
        # when a job is put with a delay, that its state is 
        # 'scheduled', when we peek it or pop it and its state is
        # now considered valid, then it should be 'waiting'
        time.freeze()
        jid = self.queue.put(Job, {'test': 'scheduled_state'}, delay=10)
        self.assertEqual(self.client.jobs[jid].state, 'scheduled')
        time.advance(11)
        self.assertEqual(self.queue.peek().state, 'waiting')
        self.assertEqual(self.client.jobs[jid].state, 'waiting')
        time.thaw()

    def test_put_pop_complete_history(self):
        # In this test, we want to put a job, pop it, and then 
        # verify that its history has been updated accordingly.
        #   1) Put a job on the queue
        #   2) Get job, check history
        #   3) Pop job, check history
        #   4) Complete job, check history
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        jid = self.queue.put(Job, {'test': 'put_history'})
        job = self.client.jobs[jid]
        self.assertEqual(math.floor(job.history[0]['put']), math.floor(time.time()))
        # Now pop it
        job = self.queue.pop()
        job = self.client.jobs[jid]
        self.assertEqual(math.floor(job.history[0]['popped']), math.floor(time.time()))

    def test_move_queue(self):
        # In this test, we want to verify that if we put a job
        # in one queue, and then move it, that it is in fact
        # no longer in the first queue.
        #   1) Put a job in one queue
        #   2) Put the same job in another queue
        #   3) Make sure that it's no longer in the first queue
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        self.assertEqual(len(self.other), 0, 'Start with an empty queue "other"')
        jid = self.queue.put(Job, {'test': 'move_queues'})
        self.assertEqual(len(self.queue), 1, 'Put failed')
        job = self.client.jobs[jid]
        job.move('other')
        self.assertEqual(len(self.queue), 0, 'Move failed')
        self.assertEqual(len(self.other), 1, 'Move failed')

    def test_move_queue_popped(self):
        # In this test, we want to verify that if we put a job
        # in one queue, it's popped, and then we move it before
        # it's turned in, then subsequent attempts to renew the
        # lock or complete the work will fail
        #   1) Put job in one queue
        #   2) Pop that job
        #   3) Put job in another queue
        #   4) Verify that heartbeats fail
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        self.assertEqual(len(self.other), 0, 'Start with an empty queue "other"')
        jid = self.queue.put(Job, {'test': 'move_queue_popped'})
        self.assertEqual(len(self.queue), 1, 'Put failed')
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        # Now move it
        job.move('other')
        self.assertRaises(LostLockException, job.heartbeat)

    def test_move_non_destructive(self):
        # In this test, we want to verify that if we move a job
        # from one queue to another, that it doesn't destroy any
        # of the other data that was associated with it. Like 
        # the priority, tags, etc.
        #   1) Put a job in a queue
        #   2) Get the data about that job before moving it
        #   3) Move it 
        #   4) Get the data about the job after
        #   5) Compare 2 and 4  
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        self.assertEqual(len(self.other), 0, 'Start with an empty queue "other"')
        jid = self.queue.put(Job, {'test': 'move_non_destructive'}, tags=['foo', 'bar'], priority=5)
        before = self.client.jobs[jid]
        before.move('other')
        after  = self.client.jobs[jid]
        self.assertEqual(before.tags    , ['foo', 'bar'])
        self.assertEqual(before.priority, 5)
        self.assertEqual(before.tags    , after.tags)
        self.assertEqual(before.data    , after.data)
        self.assertEqual(before.priority, after.priority)
        self.assertEqual(len(after.history), 2)

    def test_heartbeat(self):
        # In this test, we want to make sure that we can still 
        # keep our lock on an object if we renew it in time.
        # The gist of this test is:
        #   1) A gets an item, with positive heartbeat
        #   2) B tries to get an item, fails
        #   3) A renews its heartbeat successfully
        self.assertEqual(len(self.worker_a), 0, 'Start with an empty queue')
        jid  = self.queue.put(Job, {'test': 'heartbeat'})
        ajob = self.worker_a.pop()
        self.assertNotEqual(ajob, None)
        bjob = self.worker_b.pop()
        self.assertEqual(bjob, None)
        self.assertTrue(isinstance(ajob.heartbeat(), float))
        self.assertTrue(ajob.ttl > 0)
        # Now try setting a queue-specific heartbeat
        self.queue.heartbeat = -60
        self.assertTrue(isinstance(ajob.heartbeat(), float))
        self.assertTrue(ajob.ttl <= 0)

    def test_heartbeat_expiration(self):
        # In this test, we want to make sure that when we heartbeat a 
        # job, its expiration in the queue is also updated. So, supposing
        # that I heartbeat a job 5 times, then its expiration as far as
        # the lock itself is concerned is also updated
        self.client.config['crawl-heartbeat'] = 7200
        jid = self.queue.put(Job, {})
        job = self.worker_a.pop()
        self.assertEqual(self.worker_b.pop(), None)
        time.freeze()
        # Now, we'll advance the apparent system clock, and heartbeat
        for i in range(10):
            time.advance(3600)
            self.assertNotEqual(job.heartbeat(), False)
            self.assertEqual(self.worker_b.pop(), None)

        # Reset it to the original time object
        time.thaw()

    def test_heartbeat_state(self):
        # In this test, we want to make sure that we cannot heartbeat
        # a job that has not yet been popped
        #   1) Put a job
        #   2) DO NOT pop that job
        #   3) Ensure we cannot heartbeat that job
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        jid = self.queue.put(Job, {'test': 'heartbeat_state'})
        job = self.client.jobs[jid]
        self.assertRaises(LostLockException, job.heartbeat)

    def test_peek_pop_empty(self):
        # Make sure that we can safely pop from an empty queue
        #   1) Make sure the queue is empty
        #   2) When we pop from it, we don't get anything back
        #   3) When we peek, we don't get anything
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        self.assertEqual(self.queue.pop(), None)
        self.assertEqual(self.queue.peek(), None)

    def test_peek_attributes(self):
        # In this test, we want to put a job and peek that job, we 
        # get all the attributes back that we expect
        #   1) put a job
        #   2) peek said job, check existence of attributes
        jid = self.queue.put(Job, {'test': 'test_put_pop_attributes'})
        job = self.queue.peek()
        self.assertEqual(job.data       , {'test': 'test_put_pop_attributes'})
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.state      , 'waiting')
        self.assertEqual(job.queue_name , 'testing')
        self.assertEqual(job.queue.name , 'testing')
        self.assertEqual(job.retries_left , 5)
        self.assertEqual(job.original_retries   , 5)
        self.assertEqual(job.jid        , jid)
        self.assertEqual(job.klass_name , 'qless.job.Job')
        self.assertEqual(job.klass      , Job)
        self.assertEqual(job.tags       , [])
        jid = self.queue.put(FooJob, {'test': 'test_put_pop_attributes'})
        # Pop off the first job
        job = self.queue.pop()
        job = self.queue.peek()
        self.assertTrue('FooJob' in job.klass_name)

    def test_locks(self):
        # In this test, we're going to have two queues that point
        # to the same queue, but we're going to have them represent
        # different workers. The gist of it is this
        #   1) A gets an item, with negative heartbeat
        #   2) B gets the same item,
        #   3) A tries to renew lock on item, should fail
        #   4) B tries to renew lock on item, should succeed
        #   5) Both clean up
        jid = self.queue.put(Job, {'test': 'locks'})
        # Reset our heartbeat for both A and B
        self.client.config['heartbeat'] = -10
        # Make sure a gets a job
        ajob = self.worker_a.pop()
        self.assertNotEqual(ajob, None)
        # Now, make sure that b gets that same job
        bjob = self.worker_b.pop()
        self.assertNotEqual(bjob, None)
        self.assertEqual(ajob.jid, bjob.jid)
        self.assertTrue(isinstance(bjob.heartbeat(), float))
        self.assertTrue((bjob.heartbeat() + 11) >= time.time())
        self.assertRaises(LostLockException, ajob.heartbeat)

    def test_locks_workers(self):
        # When a worker loses a lock on a job, that job should be removed
        # from the list of jobs owned by that worker
        jid = self.queue.put(Job, {"test": "locks"}, retries=1)
        self.client.config["heartbeat"] = -10

        ajob = self.worker_a.pop()
        # Get the workers
        workers = dict((w['name'], w) for w in self.client.workers.counts)
        self.assertEqual(workers[self.worker_a.worker_name]["stalled"], 1)

        # Should have one more retry, so we should be good
        bjob = self.worker_b.pop()
        workers = dict((w['name'], w) for w in self.client.workers.counts)
        self.assertEqual(workers[self.worker_a.worker_name]["stalled"], 0)
        self.assertEqual(workers[self.worker_b.worker_name]["stalled"], 1)

        # Now it's automatically failed. Shouldn't appear in either worker
        bjob = self.worker_b.pop()
        workers = dict((w['name'], w) for w in self.client.workers.counts)
        self.assertEqual(workers[self.worker_a.worker_name]["stalled"], 0)
        self.assertEqual(workers[self.worker_b.worker_name]["stalled"], 0)

    def test_cancel(self):
        # In this test, we want to make sure that we can corretly
        # cancel a job
        #   1) Put a job
        #   2) Cancel a job
        #   3) Ensure that it's no longer in the queue
        #   4) Ensure that we can't get data for it
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        jid = self.queue.put(Job, {'test': 'cancel'})
        job = self.client.jobs[jid]
        self.assertEqual(len(self.queue), 1)
        job.cancel()
        self.assertEqual(len(self.queue), 0)
        self.assertEqual(self.client.jobs[jid], None)

    def test_cancel_heartbeat(self):
        # In this test, we want to make sure that when we cancel
        # a job, that heartbeats fail, as do completion attempts
        #   1) Put a job
        #   2) Pop that job
        #   3) Cancel that job
        #   4) Ensure that it's no longer in the queue
        #   5) Heartbeats fail, Complete fails
        #   6) Ensure that we can't get data for it
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        jid = self.queue.put(Job, {'test': 'cancel_heartbeat'})
        job = self.queue.pop()
        job.cancel()
        self.assertEqual(len(self.queue), 0)
        self.assertRaises(LostLockException, job.heartbeat)
        self.assertRaises(QlessException, job.complete)
        self.assertEqual(self.client.jobs[jid], None)

    def test_cancel_fail(self):
        # In this test, we want to make sure that if we fail a job
        # and then we cancel it, then we want to make sure that when
        # we ask for what jobs failed, we shouldn't see this one
        #   1) Put a job
        #   2) Fail that job
        #   3) Make sure we see failure stats
        #   4) Cancel that job
        #   5) Make sure that we don't see failure stats
        jid = self.queue.put(Job, {'test': 'cancel_fail'})
        job = self.queue.pop()
        job.fail('foo', 'some message')
        self.assertEqual(self.client.jobs.failed(), {'foo': 1})
        job.cancel()
        self.assertEqual(self.client.jobs.failed(), {})

    def test_complete(self):
        # In this test, we want to make sure that a job that has been
        # completed and not simultaneously enqueued are correctly 
        # marked as completed. It should have a complete history, and
        # have the correct state, no worker, and no queue
        #   1) Put an item in a queue
        #   2) Pop said item from the queue
        #   3) Complete that job
        #   4) Get the data on that job, check state
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        jid = self.queue.put(Job, {'test': 'complete'})
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        self.assertEqual(job.complete(), 'complete')
        job = self.client.jobs[jid]
        self.assertEqual(math.floor(job.history[-1]['done']), math.floor(time.time()))
        self.assertEqual(job.state      , 'complete')
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.queue_name , '')
        self.assertEqual(len(self.queue), 0)
        self.assertEqual(self.client.jobs.complete(), [jid])

        # Now, if we move job back into a queue, we shouldn't see any
        # completed jobs anymore
        job.move('testing')
        self.assertEqual(self.client.jobs.complete(), [])

    def test_complete_advance(self):
        # In this test, we want to make sure that a job that has been
        # completed and simultaneously enqueued has the correct markings.
        # It shouldn't have a worker, its history should be updated,
        # and the next-named queue should have that item.
        #   1) Put an item in a queue
        #   2) Pop said item from the queue
        #   3) Complete that job, re-enqueueing it
        #   4) Get the data on that job, check state
        #   5) Ensure that there is a work item in that queue
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        jid = self.queue.put(Job, {'test': 'complete_advance'})
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        self.assertEqual(job.complete('testing'), 'waiting')
        job = self.client.jobs[jid]
        self.assertEqual(len(job.history), 2)
        self.assertEqual(math.floor(job.history[-2]['done']), math.floor(time.time()))
        self.assertEqual(math.floor(job.history[-1]['put' ]), math.floor(time.time()))
        self.assertEqual(job.state      , 'waiting')
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.queue_name , 'testing')
        self.assertEqual(job.queue.name , 'testing')
        self.assertEqual(len(self.queue), 1)

    def test_complete_fail(self):
        # In this test, we want to make sure that a job that has been
        # handed out to a second worker can both be completed by the
        # second worker, and not completed by the first.
        #   1) Hand a job out to one worker, expire
        #   2) Hand a job out to a second worker
        #   3) First worker tries to complete it, should fail
        #   4) Second worker tries to complete it, should succeed
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        jid = self.queue.put(Job, {'test': 'complete_fail'})
        self.client.config['heartbeat'] = -10
        ajob = self.worker_a.pop()
        self.assertNotEqual(ajob, None)
        bjob = self.worker_b.pop()
        self.assertNotEqual(bjob, None)
        self.assertRaises(QlessException, ajob.complete)
        self.assertEqual(bjob.complete() , 'complete')
        job = self.client.jobs[jid]
        self.assertEqual(math.floor(job.history[-1]['done']), math.floor(time.time()))
        self.assertEqual(job.state      , 'complete')
        self.assertEqual(job.worker_name, '')
        self.assertEqual(job.queue_name , '')
        self.assertEqual(len(self.queue), 0)

    def test_complete_state(self):
        # In this test, we want to make sure that if we try to complete
        # a job that's in anything but the 'running' state.
        #   1) Put an item in a queue
        #   2) DO NOT pop that item from the queue
        #   3) Attempt to complete the job, ensure it fails
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        jid = self.queue.put(Job, {'test': 'complete_fail'})
        job = self.client.jobs[jid]
        self.assertRaises(QlessException, job.complete, 'testing')

    def test_complete_queues(self):
        # In this test, we want to make sure that if we complete a job and
        # advance it, that the new queue always shows up in the 'queues'
        # endpoint.
        #   1) Put an item in a queue
        #   2) Complete it, advancing it to a different queue
        #   3) Ensure it appears in 'queues'
        self.assertEqual(len(self.queue), 0, 'Start with an empty queue')
        jid = self.queue.put(Job, {'test': 'complete_queues'})
        self.assertEqual(len([q for q in self.client.queues.counts if q['name'] == 'other']), 0)
        self.queue.pop().complete('other')
        self.assertEqual(len([q for q in self.client.queues.counts if q['name'] == 'other']), 1)

    def test_job_time_expiration(self):
        # In this test, we want to make sure that we honor our job
        # expiration, in the sense that when jobs are completed, we 
        # then delete all the jobs that should be expired according
        # to our deletion criteria
        #   1) First, set jobs-history to -1
        #   2) Then, insert a bunch of jobs
        #   3) Pop each of these jobs
        #   4) Complete each of these jobs
        #   5) Ensure that we have no data about jobs
        self.client.config['jobs-history'] = -1
        jids = [self.queue.put(Job, {'test': 'job_time_experiation', 'count':c}) for c in range(20)]
        for c in range(len(jids)):
            self.queue.pop().complete()
        self.assertEqual(self.redis.zcard('ql:completed'), 0)
        self.assertEqual(len(self.redis.keys('ql:j:*')), 0)

    def test_job_count_expiration(self):
        # In this test, we want to make sure that we honor our job
        # expiration, in the sense that when jobs are completed, we 
        # then delete all the jobs that should be expired according
        # to our deletion criteria
        #   1) First, set jobs-history-count to 10
        #   2) Then, insert 20 jobs
        #   3) Pop each of these jobs
        #   4) Complete each of these jobs
        #   5) Ensure that we have data about 10 jobs
        self.client.config['jobs-history-count'] = 10
        jids = [self.queue.put(Job, {'test': 'job_count_expiration', 'count':c}) for c in range(20)]
        for c in range(len(jids)):
            self.queue.pop().complete()
        self.assertEqual(self.redis.zcard('ql:completed'), 10)
        self.assertEqual(len(self.redis.keys('ql:j:*')), 10)

    def test_stats_waiting(self):
        # In this test, we're going to make sure that statistics are
        # correctly collected about how long items wait in a queue
        #   1) Ensure there are no wait stats currently
        #   2) Add a bunch of jobs to a queue
        #   3) Pop a bunch of jobs from that queue, faking out the times
        #   4) Ensure that there are now correct wait stats
        stats = self.queue.stats(time.time())
        self.assertEqual(stats['wait']['count'], 0)
        self.assertEqual(stats['run' ]['count'], 0)

        time.freeze()
        jids = [self.queue.put(Job, {'test': 'stats_waiting', 'count': c}) for c in range(20)]
        self.assertEqual(len(jids), 20)
        for i in range(len(jids)):
            self.assertNotEqual(self.queue.pop(), None)
            time.advance(1)

        time.thaw()
        # Now, make sure that we see stats for the waiting
        stats = self.queue.stats(time.time())
        self.assertEqual(stats['wait']['count'], 20)
        self.assertEqual(stats['wait']['mean'] , 9.5)
        # This is our expected standard deviation
        self.assertTrue(stats['wait']['std'] - 5.916079783099 < 1e-8)
        # Now make sure that our histogram looks like what we think it
        # should
        self.assertEqual(stats['wait']['histogram'][0:20], [1] * 20)
        self.assertEqual(sum(stats['run' ]['histogram']), stats['run' ]['count'])
        self.assertEqual(sum(stats['wait']['histogram']), stats['wait']['count'])

    def test_stats_complete(self):
        # In this test, we want to make sure that statistics are
        # correctly collected about how long items take to actually 
        # get processed.
        #   1) Ensure there are no run stats currently
        #   2) Add a bunch of jobs to a queue
        #   3) Pop those jobs
        #   4) Complete those jobs, faking out the time
        #   5) Ensure that there are now correct run stats
        stats = self.queue.stats(time.time())
        self.assertEqual(stats['wait']['count'], 0)
        self.assertEqual(stats['run' ]['count'], 0)

        time.freeze()
        jids = [self.queue.put(Job, {'test': 'stats_waiting', 'count': c}) for c in range(20)]
        jobs = self.queue.pop(20)
        self.assertEqual(len(jobs), 20)
        for job in jobs:
            job.complete()
            time.advance(1)

        time.thaw()
        # Now, make sure that we see stats for the waiting
        stats = self.queue.stats(time.time())
        self.assertEqual(stats['run']['count'], 20)
        self.assertEqual(stats['run']['mean'] , 9.5)
        # This is our expected standard deviation
        self.assertTrue(stats['run']['std'] - 5.916079783099 < 1e-8)
        # Now make sure that our histogram looks like what we think it
        # should
        self.assertEqual(stats['run']['histogram'][0:20], [1] * 20)
        self.assertEqual(sum(stats['run' ]['histogram']), stats['run' ]['count'])
        self.assertEqual(sum(stats['wait']['histogram']), stats['wait']['count'])

    def test_queues(self):
        # In this test, we want to make sure that the queues function
        # can correctly identify the numbers associated with that queue
        #   1) Make sure we get nothing for no queues
        #   2) Put delayed item, check
        #   3) Put item, check
        #   4) Put, pop item, check
        #   5) Put, pop, lost item, check
        self.assertEqual(len(self.queue), 0)
        self.assertEqual(self.client.queues.counts, {})
        # Now, let's actually add an item to a queue, but scheduled
        self.queue.put(Job, {'test': 'queues'}, delay=10)
        expected = {
            'name': 'testing',
            'stalled': 0,
            'waiting': 0,
            'running': 0,
            'scheduled': 1,
            'depends': 0,
            'recurring': 0
        }
        self.assertEqual(self.client.queues.counts, [expected])
        self.assertEqual(self.client.queues["testing"].counts, expected)

        self.queue.put(Job, {'test': 'queues'})
        expected['waiting'] += 1
        self.assertEqual(self.client.queues.counts, [expected])
        self.assertEqual(self.client.queues["testing"].counts, expected)

        job = self.queue.pop()
        expected['waiting'] -= 1
        expected['running'] += 1
        self.assertEqual(self.client.queues.counts, [expected])
        self.assertEqual(self.client.queues["testing"].counts, expected)

        # Now we'll have to mess up our heartbeat to make this work
        self.queue.put(Job, {'test': 'queues'})
        self.client.config['heartbeat'] = -10
        job = self.queue.pop()
        expected['stalled'] += 1
        self.assertEqual(self.client.queues.counts, [expected])
        self.assertEqual(self.client.queues["testing"].counts, expected)

    def test_track(self):
        # In this test, we want to make sure that tracking works as expected.
        #   1) Check tracked jobs, expect none
        #   2) Put, Track a job, check
        #   3) Untrack job, check
        #   4) Track job, cancel, check
        self.assertEqual(self.client.jobs.tracked(), {'expired':{}, 'jobs':[]})
        job = self.client.jobs[self.queue.put(Job, {'test':'track'})]
        job.track()
        self.assertEqual(len(self.client.jobs.tracked()['jobs']), 1)
        job.untrack()
        self.assertEqual(len(self.client.jobs.tracked()['jobs']), 0)
        job.track()
        job.cancel()
        self.assertEqual(len(self.client.jobs.tracked()['expired']), 1)

    def test_track_tracked(self):
        # When peeked, popped, failed, etc., qless should know when a 
        # job is tracked or not
        # => 1) Put a job, track it
        # => 2) Peek, ensure tracked
        # => 3) Pop, ensure tracked
        # => 4) Fail, check failed, ensure tracked
        job = self.client.jobs[self.queue.put(Job, {'test': 'track_tracked'})]
        job.track()
        self.assertEqual(self.queue.peek().tracked, True)
        job = self.queue.pop()
        self.assertEqual(job.tracked, True)
        job.fail('foo', 'bar')
        self.assertEqual(self.client.jobs.failed('foo')['jobs'][0].tracked, True)

    def test_track_untracked(self):
        # When peeked, popped, failed, etc., qless should know when a 
        # job is not tracked
        # => 1) Put a job
        # => 2) Peek, ensure not tracked
        # => 3) Pop, ensure not tracked
        # => 4) Fail, check failed, ensure not tracked
        job = self.client.jobs[self.queue.put(Job, {'test': 'track_tracked'})]
        self.assertEqual(self.queue.peek().tracked, False)
        job = self.queue.pop()
        self.assertEqual(job.tracked, False)
        job.fail('foo', 'bar')
        self.assertEqual(self.client.jobs.failed('foo')['jobs'][0].tracked, False)

    def test_retries(self):
        # In this test, we want to make sure that jobs are given a
        # certain number of retries before automatically being considered
        # failed.
        #   1) Put a job with a few retries
        #   2) Verify there are no failures
        #   3) Lose the heartbeat as many times
        #   4) Verify there are failures
        #   5) Verify the queue is empty
        self.assertEqual(self.client.jobs.failed(), {})
        self.queue.put(Job, {'test':'retries'}, retries=2)
        # Easier to lose the heartbeat lock
        self.client.config['heartbeat'] = -10
        self.assertNotEqual(self.queue.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {})
        self.assertNotEqual(self.queue.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {})
        self.assertNotEqual(self.queue.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {})
        # This one should do it
        self.assertEqual(self.queue.pop(), None)
        self.assertEqual(self.client.jobs.failed(), {'failed-retries-testing':1})

    def test_retries_complete(self):
        # In this test, we want to make sure that jobs have their number
        # of remaining retries reset when they are put on a new queue
        #   1) Put an item with 2 retries
        #   2) Lose the heartbeat once
        #   3) Get the job, make sure it has 1 remaining
        #   4) Complete the job
        #   5) Get job, make sure it has 2 remaining
        jid = self.queue.put(Job, {'test':'retries_complete'}, retries=2)
        self.client.config['heartbeat'] = -10
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        job = self.queue.pop()
        self.assertEqual(job.retries_left, 1)
        job.complete()
        job = self.client.jobs[jid]
        self.assertEqual(job.retries_left, 2)

    def test_retries_put(self):
        # In this test, we want to make sure that jobs have their number
        # of remaining retries reset when they are put on a new queue
        #   1) Put an item with 2 retries
        #   2) Lose the heartbeat once
        #   3) Get the job, make sure it has 1 remaining
        #   4) Re-put the job in the queue with job.move
        #   5) Get job, make sure it has 2 remaining
        jid = self.queue.put(Job, {'test':'retries_put'}, retries=2)
        self.client.config['heartbeat'] = -10
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        job = self.queue.pop()
        self.assertEqual(job.retries_left, 1)
        job.move('testing')
        job = self.client.jobs[jid]
        self.assertEqual(job.retries_left, 2)

    def test_stats_failed(self):
        # In this test, we want to make sure that statistics are
        # correctly collected about how many items are currently failed
        #   1) Put an item
        #   2) Ensure we don't have any failed items in the stats for that queue
        #   3) Fail that item
        #   4) Ensure that failures and failed both increment
        #   5) Put that item back
        #   6) Ensure failed decremented, failures untouched
        jid = self.queue.put(Job, {'test':'stats_failed'})
        stats = self.queue.stats()
        self.assertEqual(stats['failed'  ], 0)
        self.assertEqual(stats['failures'], 0)
        job = self.queue.pop()
        job.fail('foo', 'bar')
        stats = self.queue.stats()
        self.assertEqual(stats['failed'  ], 1)
        self.assertEqual(stats['failures'], 1)
        job.move('testing')
        stats = self.queue.stats()
        self.assertEqual(stats['failed'  ], 0)
        self.assertEqual(stats['failures'], 1)

    def test_stats_retries(self):
        # In this test, we want to make sure that retries are getting
        # captured correctly in statistics
        #   1) Put a job
        #   2) Pop job, lose lock
        #   3) Ensure no retries in stats
        #   4) Pop job,
        #   5) Ensure one retry in stats
        jid = self.queue.put(Job, {'test':'stats_retries'})
        self.client.config['heartbeat'] = -10
        job = self.queue.pop()
        self.assertEqual(self.queue.stats()['retries'], 0)
        job = self.queue.pop()
        self.assertEqual(self.queue.stats()['retries'], 1)

    def test_stats_failed_original_day(self):
        # In this test, we want to verify that if we unfail a job on a
        # day other than the one on which it originally failed, that we
        # the `failed` stats for the original day are decremented, not
        # today.
        #   1) Put a job
        #   2) Fail that job
        #   3) Advance the clock 24 hours
        #   4) Put the job back
        #   5) Check the stats with today, check failed = 0, failures = 0
        #   6) Check 'yesterdays' stats, check failed = 0, failures = 1
        jid = self.queue.put(Job, {'test':'stats_failed_original_day'})
        job = self.queue.pop()
        job.fail('foo', 'bar')
        stats = self.queue.stats()
        self.assertEqual(stats['failures'], 1)
        self.assertEqual(stats['failed'  ], 1)

        time.freeze(); time.advance(86400)
        job.move('testing')
        # Now check tomorrow's stats
        today = self.queue.stats()
        self.assertEqual(today['failures'], 0)
        self.assertEqual(today['failed'  ], 0)
        time.thaw()
        yesterday = self.queue.stats()
        self.assertEqual(yesterday['failures'], 1)
        self.assertEqual(yesterday['failed']  , 0)

    def test_workers(self):
        # In this test, we want to verify that when we add a job, we 
        # then know about that worker, and that it correctly identifies
        # the jobs it has.
        #   1) Put a job
        #   2) Ensure empty 'workers'
        #   3) Pop that job
        #   4) Ensure unempty 'workers'
        #   5) Ensure unempty 'worker'
        jid = self.queue.put(Job, {'test':'workers'})
        self.assertEqual(self.client.workers.counts, {})
        job = self.queue.pop()
        self.assertEqual(self.client.workers.counts, [{
            'name'   : self.queue.worker_name,
            'jobs'   : 1,
            'stalled': 0
        }])
        # Now get specific worker information
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs'   : [jid],
            'stalled': []
        })

    def test_workers_cancel(self):
        # In this test, we want to verify that when a job is canceled,
        # that it is removed from the list of jobs associated with a worker
        #   1) Put a job
        #   2) Pop that job
        #   3) Ensure 'workers' and 'worker' know about it
        #   4) Cancel job
        #   5) Ensure 'workers' and 'worker' reflect that
        jid = self.queue.put(Job, {'test':'workers_cancel'})
        job = self.queue.pop()
        self.assertEqual(self.client.workers.counts, [{
            'name'   : self.queue.worker_name,
            'jobs'   : 1,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs'   : [jid],
            'stalled': []
        })
        # Now cancel the job
        job.cancel()
        self.assertEqual(self.client.workers.counts, [{
            'name'   : self.queue.worker_name,
            'jobs'   : 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs'   : [],
            'stalled': []
        })

    def test_workers_lost_lock(self):
        # In this test, we want to verify that 'workers' and 'worker'
        # correctly identify that a job is stalled, and that when that
        # job is taken from the lost lock, that it's no longer listed
        # as stalled under the original worker. Also, that workers are
        # listed in order of recency of contact
        #   1) Put a job
        #   2) Pop a job, with negative heartbeat
        #   3) Ensure 'workers' and 'worker' show it as stalled
        #   4) Pop the job with a different worker
        #   5) Ensure 'workers' and 'worker' reflect that
        jid = self.queue.put(Job, {'test':'workers_lost_lock'})
        self.client.config['heartbeat'] = -10
        job = self.queue.pop()
        self.assertEqual(self.client.workers.counts, [{
            'name'   : self.queue.worker_name,
            'jobs'   : 0,
            'stalled': 1
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs'   : [],
            'stalled': [jid]
        })
        # Now, let's pop it with a different worker
        del self.client.config['heartbeat']
        job = self.worker_a.pop()
        self.assertEqual(self.client.workers.counts, [{
            'name'   : self.worker_a.worker_name,
            'jobs'   : 1,
            'stalled': 0
        }, {
            'name'   : self.queue.worker_name,
            'jobs'   : 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs'   : [],
            'stalled': []
        })

    def test_workers_fail(self):
        # In this test, we want to make sure that when we fail a job,
        # its reflected correctly in 'workers' and 'worker'
        #   1) Put a job
        #   2) Pop job, check 'workers', 'worker'
        #   3) Fail that job
        #   4) Check 'workers', 'worker'
        jid = self.queue.put(Job, {'test':'workers_fail'})
        job = self.queue.pop()
        self.assertEqual(self.client.workers.counts, [{
            'name'   : self.queue.worker_name,
            'jobs'   : 1,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs'   : [jid],
            'stalled': []
        })
        # Now, let's fail it
        job.fail('foo', 'bar')
        self.assertEqual(self.client.workers.counts, [{
            'name'   : self.queue.worker_name,
            'jobs'   : 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs'   : [],
            'stalled': []
        })

    def test_workers_complete(self):
        # In this test, we want to make sure that when we complete a job,
        # it's reflected correctly in 'workers' and 'worker'
        #   1) Put a job
        #   2) Pop a job, check 'workers', 'worker'
        #   3) Complete job, check 'workers', 'worker'
        jid = self.queue.put(Job, {'test':'workers_complete'})
        job = self.queue.pop()
        self.assertEqual(self.client.workers.counts, [{
            'name'   : self.queue.worker_name,
            'jobs'   : 1,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs'   : [jid],
            'stalled': []
        })
        # Now complete it
        job.complete()
        self.assertEqual(self.client.workers.counts, [{
            'name'   : self.queue.worker_name,
            'jobs'   : 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs'   : [],
            'stalled': []
        })

    def test_workers_reput(self):
        # Make sure that if we move a job from one queue to another, that 
        # the job is no longer listed as one of the jobs that the worker
        # has.
        #   1) Put a job
        #   2) Pop job, check 'workers', 'worker'
        #   3) Move job, check 'workers', 'worker'
        jid = self.queue.put(Job, {'test':'workers_reput'})
        job = self.queue.pop()
        self.assertEqual(self.client.workers.counts, [{
            'name'   : self.queue.worker_name,
            'jobs'   : 1,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs'   : [jid],
            'stalled': []
        })
        job.move('other')
        self.assertEqual(self.client.workers.counts, [{
            'name'   : self.queue.worker_name,
            'jobs'   : 0,
            'stalled': 0
        }])
        self.assertEqual(self.client.workers[self.queue.worker_name], {
            'jobs'   : [],
            'stalled': []
        })

    def test_running_stalled_scheduled_depends(self):
        # Make sure that we can get a list of jids for a queue that
        # are running, stalled and scheduled
        #   1) Put a job, pop it, check 'running'
        #   2) Put a job scheduled, check 'scheduled'
        #   3) Put a job with negative heartbeat, pop, check stalled
        #   4) Put a job dependent on another and check 'depends'
        self.assertEqual(len(self.queue), 0)
        # Now, we need to check pagination
        jids = [self.queue.put(Job, {'test': 'rssd'}) for i in range(20)]
        self.client.config['heartbeat'] = -60
        jobs = self.queue.pop(20)
        self.assertEqual(set(self.queue.jobs.stalled(0, 10) + self.queue.jobs.stalled(10, 10)), set(jids))

        self.client.config['heartbeat'] = 60
        jobs = self.queue.pop(20)
        self.assertEqual(set(self.queue.jobs.running(0, 10) + self.queue.jobs.running(10, 10)), set(jids))

        # Complete all these jobs
        r = [job.complete() for job in jobs]
        jids = [job.jid for job in jobs]
        jids.reverse()
        self.assertEqual(self.client.jobs.complete(0, 10) + self.client.jobs.complete(10, 10), jids)

        jids = [self.queue.put(Job, {'test': 'rssd'}, delay=60) for i in range(20)]
        self.assertEqual(set(self.queue.jobs.scheduled(0, 10) + self.queue.jobs.scheduled(10, 10)), set(jids))

        jids = [self.queue.put(Job, {'test': 'rssd'}, depends=jids) for i in range(20)]
        self.assertEqual(set(self.queue.jobs.depends(0, 10) + self.queue.jobs.depends(10, 10)), set(jids))

if __name__ == '__main__':
    unittest.main()
