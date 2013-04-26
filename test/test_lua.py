#! /usr/bin/env python

from common import TestQless, FooJob

import time
import math
import unittest

# Qless dependencies
from qless import Job
from qless.exceptions import LostLockException, QlessException


class TestEverything(TestQless):
    '''In these tests, we want to ensure that if we don't provide enough or
    correctly-formatted arguments to the lua scripts, that they'll barf on us
    like we ask.'''

    def helper(self, command, args):
        '''Help by running tests'''
        for arg in args:
            self.assertRaises(Exception, self.client, command, *arg)

    def test_lua_complete(self):
        '''Ensure that the complete lua scripts raises argument errors'''
        self.helper('complete', [
            # Not enough args
            [],
            # Missing worker
            ['deadbeef'],
            # Missing queue
            ['deadbeef', 'worker1'],
            # Malformed JSON
            ['deadbeef', 'worker1', 'foo', '[}'],
            # Not a number for delay
            ['deadbeef', 'worker1', 'foo', '{}', 'next', 'howdy', 'delay',
                'howdy'],
            # Mutually exclusive options
            ['deadbeef', 'worker1', 'foo', '{}', 'next', 'foo', 'delay', 5,
                'depends', '["foo"]'],
            # Mutually inclusive options (with 'next')
            ['deadbeef', 'worker1', 'foo', '{}', 'delay', 5],
            ['deadbeef', 'worker1', 'foo', '{}', 'depends', '["foo"]']
        ])

    def test_lua_fail(self):
        '''Fail script should raise argument errors'''
        self.helper('fail', [
            # Missing id
            [],
            # Missing worker
            ['deadbeef'],
            # Missing type
            ['deadbeef', 'worker1'],
            # Missing message
            ['deadbeef', 'worker1', 'foo'],
            # Malformed data
            ['deadbeef', 'worker1', 'foo', 'bar', '[}']
        ])

    def test_lua_failed(self):
        '''Failed script should raise argument errors'''
        self.helper('failed', [
            # Malformed start
            ['bar', 'howdy'],
            # Malformed limit
            ['bar', 0, 'howdy'],
        ])

    def test_lua_get(self):
        '''The get script should raise argument errors'''
        self.helper('get', [
            # Missing jid
            []
        ])

    def test_lua_heartbeat(self):
        '''The heartbeat function should raise argument errors'''
        self.helper('heartbeat', [
            # Missing id
            [],
            # Missing worker
            ['deadbeef'],
            # Missing expiration
            ['deadbeef', 'worker1'],
            # Malformed JSON
            ['deadbeef', 'worker1', '[}']
        ])

    def test_lua_jobs(self):
        '''Jobs function should raise argument errors'''
        self.helper('jobs', [
            # Unrecognized option
            ['testing'],
            # Missing queue
            ['stalled']
        ])

    def test_lua_peek(self):
        '''Peek function should raise argument errors'''
        self.helper('peek', [
            # Missing count
            [],
            # Malformed count
            ['howdy']
        ])

    def test_lua_pop(self):
        '''Pop function should raise argument errors'''
        self.helper('pop', [
            # Missing worker
            [],
            # Missing count
            ['worker1'],
            # Malformed count
            ['worker1', 'howdy'],
        ])

    def test_lua_priority(self):
        '''Priority function should raise argument errors'''
        self.helper('priority', [
            # Missing jid
            [],
            # Missing priority
            ['12345'],
            # Malformed priority
            ['12345', 'howdy'],
        ])

    def test_lua_put(self):
        '''Put function should raise argument errors'''
        self.helper('put', [
            # Missing id
            [],
            # Missing klass
            ['deadbeef'],
            # Missing data
            ['deadbeef', 'foo'],
            # Malformed data
            ['deadbeef', 'foo', '[}'],
            # Non-dictionary data
            ['deadbeef', 'foo', '[]'],
            # Non-dictionary data
            ['deadbeef', 'foo', '"foobar"'],
            # Malformed delay
            ['deadbeef', 'foo', '{}', 'howdy'],
            # Malformed priority
            ['deadbeef', 'foo', '{}', 0, 'priority', 'howdy'],
            # Malformed tags
            ['deadbeef', 'foo', '{}', 0, 'tags', '[}'],
            # Malformed retries
            ['deadbeef', 'foo', '{}', 0, 'retries', 'hello'],
            # Mutually exclusive options
            ['deadbeef', 'foo', '{}', 5, 'depends', '["hello"]']
        ])

    def test_lua_recur(self):
        '''Recur function should raise argument errors'''
        self.helper('recur.on', [
            [],
            ['testing'],
            ['testing', 'foo.klass'],
            ['testing', 'foo.klass', '{}'],
            ['testing', 'foo.klass', '{}', 12345],
            ['testing', 'foo.klass', '{}', 12345, 'interval'],
            ['testing', 'foo.klass', '{}', 12345, 'interval', 12345],
            ['testing', 'foo.klass', '{}', 12345, 'interval', 12345, 0],
            # Malformed data, priority, tags, retries
            ['testing', 'foo.klass', '[}', 12345, 'interval', 12345, 0],
            ['testing', 'foo.klass', '{}', 12345, 'interval', 12345, 0,
                'priority', 'foo'],
            ['testing', 'foo.klass', '{}', 12345, 'interval', 12345, 0,
                'retries', 'foo'],
            ['testing', 'foo.klass', '{}', 12345, 'interval', 12345, 0,
                'tags', '[}'],
        ])

        # Canceling recurring jobs
        self.helper('recur.off', [
            # Missing jid
            []
        ])

        # Getting recurring jobs
        self.helper('recur.get', [
            # Missing jid
            []
        ])

        # Updating a recurring job
        self.helper('recur.update', [
            ['update'],
            ['update', 'priority', 'foo'],
            ['update', 'interval', 'foo'],
            ['update', 'retries', 'foo'],
            ['update', 'data', '[}'],
        ])

    def test_lua_retry(self):
        '''Retry script should raise arugment errors'''
        self.helper('retry', [
            # Missing queue
            ['12345'],
            # Missing worker
            ['12345', 'testing'],
            # Malformed delay
            ['12345', 'testing', 'worker', 'howdy'],
        ])

    def test_lua_stats(self):
        '''Stats function should raise argument errors'''
        self.helper('stats', [
            # Missing queue
            [],
            # Missing date
            ['foo']
        ])

    def test_lua_tag(self):
        '''Tag function should raise argument errors'''
        self.helper('tag', [
            # Missing jid
            ['add'],
            # Missing jid
            ['remove'],
            # Missing tag
            ['get'],
            # Malformed offset
            ['get', 'foo', 'howdy'],
            # Malformed count
            ['get', 'foo', 0, 'howdy']
        ])

    def test_lua_track(self):
        '''Track function should raise argument errors'''
        self.helper('track', [
            # Unknown command
            ['fslkdjf', 'deadbeef'],
            # Missing jid
            ['track']
        ])

if __name__ == '__main__':
    unittest.main()
