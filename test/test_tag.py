#! /usr/bin/env python

from common import TestQless, FooJob

import time
import math
import unittest
import qless
from qless.exceptions import LostLockException, QlessException


class TestTag(TestQless):
    # 1) Should make sure that when we double-tag an item, that we don't
    #   see it show up twice when we get it back with the job
    # 2) Should also preserve tags in the order in which they were inserted
    # 3) When a job expires or is canceled, it should be removed from the 
    #   set of jobs with that tag
    def test_tag(self):
        job = self.client.jobs[self.queue.put(qless.Job, {'test': 'tag'})]
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 0, 'jobs': {}})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 0, 'jobs': {}})
        job.tag('foo')
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 1, 'jobs': [job.jid]})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 0, 'jobs': {}})
        job.tag('bar')
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 1, 'jobs': [job.jid]})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 1, 'jobs': [job.jid]})
        job.untag('foo')
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 0, 'jobs': {}})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 1, 'jobs': [job.jid]})
        job.untag('bar')
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 0, 'jobs': {}})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 0, 'jobs': {}})

    def test_preserve_order(self):
        job = self.client.jobs[self.queue.put(qless.Job, {'test': 'preserve_order'})]
        tags = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
        for i in range(len(tags)):
            job.tag(tags[i])
            self.assertEqual(self.client.jobs[job.jid].tags, tags[0:i+1])

        # Now let's take a select few out
        job.untag('a', 'c', 'e', 'g')
        self.assertEqual(self.client.jobs[job.jid].tags, ['b', 'd', 'f', 'h'])

    def test_cancel_expire(self):
        # First, we'll cancel a job
        job = self.client.jobs[self.queue.put(qless.Job, {'test': 'cancel_expire'})]
        job.tag('foo', 'bar')
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 1, 'jobs': [job.jid]})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 1, 'jobs': [job.jid]})
        job.cancel()
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 0, 'jobs': {}})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 0, 'jobs': {}})

        # Now, we'll have a job expire from completion
        self.client.config['jobs-history-count'] = 0
        self.queue.put(qless.Job, {'test': 'cancel_expire'})
        job = self.queue.pop()
        self.assertNotEqual(job, None)
        job.tag('foo', 'bar')
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 1, 'jobs': [job.jid]})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 1, 'jobs': [job.jid]})
        self.assertEqual(job.complete(), 'complete')
        self.assertEqual(self.client.jobs[job.jid], None)
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 0, 'jobs': {}})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 0, 'jobs': {}})

        # If the job no longer exists, attempts to tag it should not add to the set
        job.tag('foo', 'bar')
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 0, 'jobs': {}})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 0, 'jobs': {}})

    def test_tag_put(self):
        # We should make sure that we can tag a job when we initially put it, too
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 0, 'jobs': {}})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 0, 'jobs': {}})
        jid = self.queue.put(qless.Job, {'test': 'tag_put'}, tags=['foo', 'bar'])
        self.assertEqual(self.client.jobs.tagged('foo'), {'total': 1, 'jobs': [jid]})
        self.assertEqual(self.client.jobs.tagged('bar'), {'total': 1, 'jobs': [jid]})

    def test_tag_top(self):
        # 1) Make sure that it only includes tags with more than one job associated with it
        # 2) Make sure that when jobs are untagged, it decrements the count
        # 3) When we tag a job, it increments the count
        # 4) When jobs complete and expire, it decrements the count
        # 5) When jobs are put, make sure it shows up in the tags
        # 6) When canceled, decrements
        self.assertEqual(self.client.tags(), {})
        jids = [self.queue.put(qless.Job, {}, tags=['foo']) for i in range(10)]
        self.assertEqual(self.client.tags(), ['foo'])
        jobs = [self.client.jobs[jid].cancel() for jid in jids]
        self.assertEqual(self.client.tags(), {})
        # Add only one back
        a = self.queue.put(qless.Job, {}, tags=['foo'])
        self.assertEqual(self.client.tags(), {})
        # Add a second, and then tag it
        b = self.client.jobs[self.queue.put(qless.Job, {})]
        b.tag('foo')
        self.assertEqual(self.client.tags(), ['foo'])
        b.untag('foo')
        self.assertEqual(self.client.tags(), {})
        b.tag('foo')
        # Test job expiration
        self.client.config['jobs-history-count'] = 0
        self.assertEqual(len(self.queue), 2)
        self.queue.pop().complete()
        self.assertEqual(self.client.tags(), {})

if __name__ == '__main__':
    unittest.main()
