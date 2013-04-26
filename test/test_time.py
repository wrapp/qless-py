#! /usr/bin/env python

'''Make sure our time freeze method works'''

import unittest
from hammertime import HammerTime


class TestHammerTime(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.time = HammerTime()

    def tearDown(self):
        '''Always make sure that we thaw time between tests'''
        self.time.thaw()

    def test_basic(self):
        '''Make sure that time freezing works in the most basic way'''
        # Now, let's freeze time and verify that we cannot witness the passage
        # of time when time is frozen
        for count in xrange(100):
            first = self.time.time()
            self.time.freeze()
            before = self.time.time()
            after = self.time.time()
            self.assertGreater(before, first)
            self.assertGreater(after, first)
            self.assertEqual(before, after)

    def test_context_manager(self):
        '''Ensure that this works like a context manager as well'''
        # Ensure that the times are not equal before
        times = [self.time.time() for i in xrange(1000)]
        self.assertNotEqual([times[0]] * 1000, times)

        with self.time:
            # Make sure all the times we capture are equal to the first
            times = [self.time.time() for i in xrange(1000)]
            self.assertEqual([times[0]] * 1000, times)

        # Ensure that again the times are not equal after the context
        times = [self.time.time() for i in xrange(1000)]
        self.assertNotEqual([times[0]] * 1000, times)

    def test_advance(self):
        '''Make sure that we can both advance and rewind time'''
        with self.time as time:
            # We'll use the advance function
            before = int(time.time()) + 1
            time.advance(1)
            now = int(time.time())
            self.assertEqual(before, now)

            # We'll rewind a bit
            before = int(time.time()) - 1
            time.advance(-1)
            now = int(time.time())
            self.assertEqual(before, now)

            # We'll use the add and subtract methods now
            before = int(time.time()) + 1
            time += 1
            now = int(time.time())
            self.assertEqual(before, now)

            before = int(time.time()) - 1
            time -= 1
            now = int(time.time())
            self.assertEqual(before, now)

if __name__ == '__main__':
    unittest.main()
