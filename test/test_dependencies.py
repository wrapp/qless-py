#! /usr/bin/env python

from common import TestQless, FooJob

import time
import math
import unittest

# Qless dependencies
import qless
from qless.exceptions import LostLockException, QlessException


class TestDependencies(TestQless):
    def test_depends_put(self):
        # In this test, we want to put a job, and put a second job
        # that depends on it. We'd then like to verify that it's 
        # only available for popping once its dependency has completed
        jid = self.queue.put(qless.Job, {'test': 'depends_put'})
        job = self.queue.pop()
        jid = self.queue.put(qless.Job, {'test': 'depends_put'}, depends=[job.jid])
        self.assertEqual(self.queue.pop(), None)
        self.assertEqual(self.client.jobs[jid].state, 'depends')
        job.complete()
        self.assertEqual(self.client.jobs[jid].state, 'waiting')
        self.assertEqual(self.queue.pop().jid, jid)

        # Let's try this dance again, but with more job dependencies
        jids = [self.queue.put(qless.Job, {'test': 'depends_put'}) for i in range(10)]
        jid  = self.queue.put(qless.Job, {'test': 'depends_put'}, depends=jids)
        # Pop more than we put on
        jobs = self.queue.pop(20)
        self.assertEqual(len(jobs), 10)
        # Complete them, and then make sure the last one's available
        for job in jobs:
            self.assertEqual(self.queue.pop(), None)
            job.complete()

        # It's only when all the dependencies have been completed that
        # we should be able to pop this job off
        self.assertEqual(self.queue.pop().jid, jid)

    def test_depends_complete(self):
        # In this test, we want to put a job, put a second job, and
        # complete the first job, making it dependent on the second
        # job. This should test the ability to add dependency during
        # completion
        a = self.queue.put(qless.Job, {'test': 'depends_complete'})
        b = self.queue.put(qless.Job, {'test': 'depends_complete'})
        job = self.queue.pop()
        job.complete('testing', depends=[b])
        self.assertEqual(self.client.jobs[a].state, 'depends')
        jobs = self.queue.pop(20)
        self.assertEqual(len(jobs), 1)
        jobs[0].complete()
        self.assertEqual(self.client.jobs[a].state, 'waiting')
        job = self.queue.pop()
        self.assertEqual(job.jid, a)

        # Like above, let's try this dance again with more dependencies
        jids = [self.queue.put(qless.Job, {'test': 'depends_put'}) for i in range(10)]
        jid  = job.jid
        job.complete('testing', depends=jids)
        # Pop more than we put on
        jobs = self.queue.pop(20)
        self.assertEqual(len(jobs), 10)
        # Complete them, and then make sure the last one's available
        for job in jobs:
            j = self.queue.pop()
            self.assertEqual(j, None)
            job.complete()

        # It's only when all the dependencies have been completed that
        # we should be able to pop this job off
        self.assertEqual(self.queue.pop().jid, jid)

    def test_depends_state(self):
        # Put a job, and make it dependent on a canceled job, and a
        # non-existent job, and a complete job. It should be available
        # from the start.
        jids = ['foobar', 
            self.queue.put(qless.Job, {'test': 'test_depends_state'}),
            self.queue.put(qless.Job, {'test': 'test_depends_state'})]

        # Cancel one, complete one
        self.queue.pop().cancel()
        self.queue.pop().complete()
        # Ensure there are none in the queue, then put one, should pop right off
        self.assertEqual(len(self.queue), 0)
        jid = self.queue.put(qless.Job, {'test': 'test_depends_state'}, depends=jids)
        self.assertEqual(self.queue.pop().jid, jid)

    def test_depends_canceled(self):
        # B is dependent on A, but then we cancel B, then A is still
        # able to complete without any problems. If you try to cancel
        # a job that others depend on, you should have an exception thrown
        a = self.queue.put(qless.Job, {'test': 'test_depends_canceled'})
        b = self.queue.put(qless.Job, {'test': 'test_depends_canceled'}, depends=[a])
        self.client.jobs[b].cancel()
        job = self.queue.pop()
        self.assertEqual(job.jid, a)
        self.assertEqual(job.complete(), 'complete')
        self.assertEqual(self.queue.pop(), None)

        a = self.queue.put(qless.Job, {'test': 'cancel_dependency'})
        b = self.queue.put(qless.Job, {'test': 'cancel_dependency'}, depends=[a])
        try:
            self.assertTrue(self.client.jobs[a].cancel(), 'We should not be able to cancel jobs with dependencies')
        except Exception as e:
            self.assertTrue('Cancel()' in e.message, 'Cancel() threw the wrong error')

        # When canceling a job, we should remove that job from the jobs' list
        # of dependents.
        self.client.jobs[b].cancel()
        self.assertEqual(self.client.jobs[a].dependents, [])
        # We should also just be able to cancel a now
        self.client.jobs[a].cancel()

    def test_depends_complete_advance(self):
        # If we make B depend on A, and then move A through several
        # queues, then B should only be availble once A has finished
        # its whole run.
        a = self.queue.put(qless.Job, {'test': 'test_depends_advance'})
        b = self.queue.put(qless.Job, {'test': 'test_depends_advance'}, depends=[a])
        for i in range(10):
            job = self.queue.pop()
            self.assertEqual(job.jid, a)
            job.complete('testing')

        self.queue.pop().complete()
        self.assertEqual(self.queue.pop().jid, b)

    def test_cascading_dependency(self):
        # If we make a dependency chain, then we validate that we can
        # only access them one at a time, in the order of their dependency
        jids = [self.queue.put(qless.Job, {'test': 'cascading_depencency'})]
        for i in range(10):
            jids.append(self.queue.put(qless.Job, {'test': 'cascading_dependency'}, depends=[jids[-1]]))

        # Pop off the first 10 dependencies, ensuring only one comes off at a time
        for i in range(11):
            jobs = self.queue.pop(10)
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0].jid, jids[i])
            jobs[0].complete()

    def test_move_dependency(self):
        # If we put a job into a queue with dependencies, and then 
        # move it to another queue, then all the original dependencies
        # should be honored. The reason for this is that dependencies
        # can always be removed after the fact, but this prevents us
        # from the running the risk of moving a job, and it getting 
        # popped before we can describe its dependencies
        a = self.queue.put(qless.Job, {'test': 'move_dependency'})
        b = self.queue.put(qless.Job, {'test': 'move_dependency'}, depends=[a])
        self.client.jobs[b].move('other')
        self.assertEqual(self.client.jobs[b].state, 'depends')
        self.assertEqual(self.other.pop(), None)
        self.queue.pop().complete()
        self.assertEqual(self.client.jobs[b].state, 'waiting')
        self.assertEqual(self.other.pop().jid, b)

    def test_add_dependency(self):
        # If we have a job that already depends on on other jobs, then
        # we should be able to add more dependencies. If it's not, then
        # we can't
        a = self.queue.put(qless.Job, {'test': 'add_dependency'})
        b = self.client.jobs[self.queue.put(qless.Job, {'test': 'add_dependency'}, depends=[a])]
        c = self.queue.put(qless.Job, {'test': 'add_dependency'})
        self.assertEqual(b.depend(c), True)

        jobs = self.queue.pop(20)
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0].jid, a)
        self.assertEqual(jobs[1].jid, c)
        jobs[0].complete(); jobs[1].complete()
        job = self.queue.pop()
        self.assertEqual(job.jid, b.jid)
        job.complete()

        # If the job's put, but waiting, we can't add dependencies
        a = self.queue.put(qless.Job, {'test': 'add_dependency'})
        b = self.queue.put(qless.Job, {'test': 'add_dependency'})
        self.assertEqual(self.client.jobs[a].depend(b), False)
        job = self.queue.pop()
        self.assertEqual(job.depend(b), False)
        job.fail('what', 'something')
        self.assertEqual(self.client.jobs[job.jid].depend(b), False)

    def test_remove_dependency(self):
        # If we have a job that already depends on others, then we should
        # we able to remove them. If it's not dependent on any, then we can't.
        a = self.queue.put(qless.Job, {'test': 'remove_dependency'})
        b = self.client.jobs[self.queue.put(qless.Job, {'test': 'remove_dependency'}, depends=[a])]
        self.assertEqual(len(self.queue.pop(20)), 1)
        b.undepend(a)
        self.assertEqual(self.queue.pop().jid, b.jid)
        # Make sure we removed the dependents from the first one, as well
        self.assertEqual(self.client.jobs[a].dependents, [])

        # Let's try removing /all/ dependencies
        jids = [self.queue.put(qless.Job, {'test': 'remove_dependency'}) for i in range(10)]
        b = self.client.jobs[self.queue.put(qless.Job, {'test': 'remove_dependency'}, depends=jids)]
        self.assertEqual(len(self.queue.pop(20)), 10)
        b.undepend(all=True)
        self.assertEqual(self.client.jobs[b.jid].state, 'waiting')
        # Let's make sure that each of the jobs we removed as dependencies also go their
        # dependencies removed, too.
        for jid in jids:
            self.assertEqual(self.client.jobs[jid].dependents, [])

        # If the job's put, but waiting, we can't add dependencies
        a = self.queue.put(qless.Job, {'test': 'add_dependency'})
        b = self.queue.put(qless.Job, {'test': 'add_dependency'})
        self.assertEqual(self.client.jobs[a].undepend(b), False)
        job = self.queue.pop()
        self.assertEqual(job.undepend(b), False)
        job.fail('what', 'something')
        self.assertEqual(self.client.jobs[job.jid].undepend(b), False)

    def test_jobs_depends(self):
        # When we have jobs that have dependencies, we should be able to
        # get access to them.
        a = self.queue.put(qless.Job, {'test': 'jobs_depends'})
        b = self.queue.put(qless.Job, {'test': 'jobs_depends'}, depends=[a])
        self.assertEqual(self.client.queues.counts[0]['depends'], 1)
        self.assertEqual(self.client.queues['testing'].counts['depends'], 1)
        self.assertEqual(self.queue.jobs.depends(), [b])

        # When we remove a dependency, we should no longer see that job as a dependency
        self.client.jobs[b].undepend(a)
        self.assertEqual(self.client.queues.counts[0]['depends'], 0)
        self.assertEqual(self.client.queues['testing'].counts['depends'], 0)
        self.assertEqual(self.queue.jobs.depends(), [])

        # When we move a job that has a dependency, we should no longer
        # see it in the depends() of the original job
        a = self.queue.put(qless.Job, {'test': 'jobs_depends'})
        b = self.queue.put(qless.Job, {'test': 'jobs_depends'}, depends=[a])
        self.assertEqual(self.client.queues.counts[0]['depends'], 1)
        self.assertEqual(self.client.queues['testing'].counts['depends'], 1)
        self.assertEqual(self.queue.jobs.depends(), [b])
        # Now, move the job
        self.client.jobs[b].move('other')
        self.assertEqual(self.client.queues.counts[0]['depends'], 0)
        self.assertEqual(self.client.queues['testing'].counts['depends'], 0)
        self.assertEqual(self.queue.jobs.depends(), [])

if __name__ == '__main__':
    unittest.main()
