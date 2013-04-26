#! /usr/bin/env python

'''All the python-client-specific tests'''

from common import TestQless
import unittest
import qless


class BarJob(object):
    '''A dummy job type'''
    @staticmethod
    def other(job):
        '''Run the job out of the 'other' queue'''
        job.fail('foo', 'bar')

    @staticmethod
    def process(job):
        '''Run the job itself'''
        job.complete()


class FailJob(object):
    '''A dummy job type'''
    @staticmethod
    def testing(job):
        '''Run the job in the 'testing' queue'''
        raise Exception('foo')

    def other(self, job):
        '''This isn't a static method like it should be'''
        job.complete()

class MissingJob(object):
    # This class doesn't have any appropriate methods
    pass


class TestPython(TestQless):
    def test_job(self):
        job = self.client.jobs[self.queue.put(qless.Job, {})]
        self.assertTrue(job.jid in str(job))
        self.assertTrue(job.jid in repr(job))
        self.assertRaises(AttributeError, lambda: job.foo)
        job['testing'] = 'foo'
        self.assertEqual(job['testing'], 'foo')

    def test_job_by_string(self):
        job = self.client.jobs[self.queue.put('test.BarJob', {})]
        self.assertTrue(job.jid in str(job))
        self.assertTrue(job.jid in repr(job))
        self.assertRaises(AttributeError, lambda: job.foo)
        job['testing'] = 'foo'
        self.assertEqual(job['testing'], 'foo')

    def test_job_by_unicode(self):
        job = self.client.jobs[self.queue.put(u'test.BarJob', {})]
        self.assertTrue(job.jid in str(job))
        self.assertTrue(job.jid in repr(job))
        self.assertRaises(AttributeError, lambda: job.foo)
        job['testing'] = 'foo'
        self.assertEqual(job['testing'], 'foo')

    def test_queue(self):
        self.assertRaises(AttributeError, lambda: self.queue.foo)

    def test_process(self):
        jid = self.queue.put(BarJob, {})
        self.queue.pop().process()
        self.assertEqual(self.client.jobs[jid].state, 'complete')

        jid = self.other.put(BarJob, {})
        self.other.pop().process()
        self.assertEqual(self.client.jobs[jid].state, 'failed')

        jid = self.queue.put(FailJob, {})
        self.queue.pop().process()
        self.assertEqual(self.client.jobs[jid].state, 'failed')

        jid = self.other.put(FailJob, {})
        self.other.pop().process()
        self.assertEqual(self.client.jobs[jid].state, 'failed')
        self.client.jobs[jid].move('other')
        FailJob().other(self.other.pop())
        self.assertEqual(self.client.jobs[jid].state, 'complete')

        jid = self.queue.put(MissingJob, {})
        self.queue.pop().process()
        self.assertEqual(self.client.jobs[jid].state, 'failed')

    def test_config(self):
        c = self.client.config
        before = len(c)
        c['foo'] = 'bar'
        self.assertEqual(before + 1, len(c))
        c.clear()
        self.assertEqual(before, len(c))

        self.assertEqual([i for i in c], c.keys(), c.all.keys())
        self.assertIn('jobs-history', c)
        self.assertNotIn('foo', c)
        c['foo'] = 'bar'
        self.assertIn('foo', c)
        c.clear()

        self.assertEqual(c.get('foo', 'bar'), 'bar')
        c['foo'] = 'hey'
        self.assertEqual(c.get('foo', 'bar'), 'hey')
        c.clear()

        self.assertEqual(c.items(), c.all.items())
        c['foo'] = 'bar'
        self.assertEqual(c.pop('foo', 'hey'), 'bar')
        self.assertNotIn('foo', c)
        self.assertEqual(c.pop('foo', 'hey'), 'hey')
        c.clear()

        c.update({'foo': 'bar', 'whiz': 'bang'})
        self.assertEqual(c.pop('foo' ), 'bar' )
        self.assertEqual(c.pop('whiz'), 'bang')
        c.update([('foo', 'bar'), ('whiz', 'bang')])
        self.assertEqual(c.pop('foo' ), 'bar' )
        self.assertEqual(c.pop('whiz'), 'bang')
        c.update(foo='bar', whiz='bang')
        self.assertEqual(c.pop('foo' ), 'bar' )
        self.assertEqual(c.pop('whiz'), 'bang')

        self.assertEqual(c.values(), c.all.values())

if __name__ == '__main__':
    unittest.main()
