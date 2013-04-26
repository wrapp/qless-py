'''A base class for all of our common tests'''

# First things first, we need to monkey-patch time
import hammertime
hammertime.patch()

import time
import redis
import unittest

# Qless stuff
import qless
from qless.exceptions import LostLockException, QlessException


class FooJob(qless.Job):
    pass


class TestQless(unittest.TestCase):
    '''Base class for all of our tests'''
    @classmethod
    def setUpClass(cls):
        # We can set the log level here. It's useful for debugging
        from qless import logger
        import logging
        #logger.setLevel(logging.DEBUG)
        cls.redis = redis.Redis()
        # The qless client we're using
        cls.client = qless.client()

    def setUp(self):
        assert(len(self.redis.keys('*')) == 0)
        # Clear the script cache, and nuke everything
        self.redis.execute_command('script', 'flush')
        # Our main queue
        self.queue = self.client.queues['testing']

        # This represents worker 'a'
        tmp = qless.client()
        tmp.worker_name = 'worker-a'
        self.worker_a = tmp.queues['testing']

        # This represents worker b
        tmp = qless.client()
        tmp.worker_name = 'worker-b'
        self.worker_b = tmp.queues['testing']

        # This is just a second queue
        self.other = self.client.queues['other']

    def tearDown(self):
        # Ensure that we leave no keys behind, and that we've unfrozen time
        self.redis.flushdb()
        time.thaw()
