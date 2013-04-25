#! /usr/bin/env python

import time
import redis
import pkgutil
import logging
import simplejson as json

# Internal imports
from .exceptions import QlessException


logger = logging.getLogger('qless')
formatter = logging.Formatter(
    '%(asctime)s | PID %(process)d | [%(levelname)s] %(message)s')
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.FATAL)


# A decorator to specify a bunch of exceptions that should be caught
# and the job retried. It turns out this comes up with relative frequency
def retry(*excepts):
    def decorator(f):
        def _f(job):
            try:
                f(job)
            except tuple(excepts):
                job.retry()
        return _f
    return decorator


class Jobs(object):
    '''Class for accessing jobs and job information lazily'''
    def __init__(self, client):
        self.client = client

    def complete(self, offset=0, count=25):
        '''Return the paginated jids of complete jobs'''
        return self.client('jobs', 'complete', offset, count)

    def tracked(self):
        '''Return an array of job objects that are being tracked'''
        results = json.loads(self.client('track'))
        results['jobs'] = [Job(self, **job) for job in results['jobs']]
        return results

    def tagged(self, tag, offset=0, count=25):
        '''Return the paginated jids of jobs tagged with a tag'''
        return json.loads(self.client('tag', 'get', tag, offset, count))

    def failed(self, group=None, start=0, limit=25):
        '''If no group is provided, this returns a JSON blob of the counts of
        the various types of failures known. If a type is provided, returns
        paginated job objects affected by that kind of failure.'''
        if not group:
            return json.loads(self.client('failed'))
        else:
            results = json.loads(
                self.client('failed', group, start, limit))
            results['jobs'] = [Job(self.client, **j) for j in results['jobs']]
            return results

    def __getitem__(self, jid):
        '''Get a job object corresponding to that jid, or ``None`` if it
        doesn't exist'''
        results = self.client('get', jid)
        if not results:
            results = json.loads(
                self.client('recur.get', jid))
            if not results:
                return None
            return RecurringJob(self.client, **results)
        return Job(self.client, **json.loads(results))


class Workers(object):
    '''Class for accessing worker information lazily'''
    def __init__(self, clnt):
        self.client = clnt

    def __getattr__(self, attr):
        '''What workers are workers, and how many jobs are they running'''
        if attr == 'counts':
            return json.loads(self.client('workers'))
        raise AttributeError('qless.Workers has no attribute %s' % attr)

    def __getitem__(self, worker_name):
        '''Which jobs does a particular worker have running'''
        result = json.loads(
            self.client('workers', worker_name))
        result['jobs']    = result['jobs'] or []
        result['stalled'] = result['stalled'] or []
        return result

class Queues(object):
    '''Class for accessing queues lazily'''
    def __init__(self, clnt):
        self.client = clnt

    def __getattr__(self, attr):
        '''What queues are there, and how many jobs do they have running,
        waiting, scheduled, etc.'''
        if attr == 'counts':
            return json.loads(self.client('queues'))
        raise AttributeError('qless.Queues has no attribute %s' % attr)

    def __getitem__(self, queue_name):
        '''Get a queue object associated with the provided queue name'''
        return Queue(queue_name, self.client, self.client.worker_name)


class Events(object):
    '''A class for handling pubsub events'''
    def __init__(self, clnt):
        self.pubsub = clnt.redis.pubsub()
        self.callbacks = dict(
            (k, None) for k in (
                'canceled', 'completed', 'failed', 'popped', 'stalled', 'put',
                'track', 'untrack')
        )
        for key in self.callbacks.keys():
            self.pubsub.subscribe(key)

    def next(self):
        '''Wait for the next pubsub event'''
        message = self.pubsub.listen().next()
        func = (
                message and
                message['type'] == 'message' and
                self.callbacks.get(message['channel']))
        if func:
            func(message['data'])

    def listen(self):
        '''Listen for events as they come in'''
        try:
            while True:
                self.next()
        except redis.ConnectionError:
            return

    def on(self, evt, func):
        '''Set a callback handler for a pubsub event'''
        if evt not in self.callbacks:
            raise NotImplementedError('callback "%s"' % evt)
        else:
            self.callbacks[evt] = func

    def off(self, evt):
        '''Deactivate the callback for a pubsub event'''
        return self.callbacks.pop(evt, None)


class client(object):
    '''Basic qless client object.'''
    def __init__(self, host='localhost', port=6379, hostname=None, **kwargs):
        import socket
        # This is our unique idenitifier as a worker
        self.worker_name = hostname or socket.gethostname()
        # This is just the redis instance we're connected to
        # conceivably someone might want to work with multiple
        # instances simultaneously.
        self.redis   = redis.Redis(host, port, **kwargs)
        self.config  = Config(self)
        self.jobs    = Jobs(self)
        self.workers = Workers(self)
        self.queues  = Queues(self)

        # We now have a single unified core script.
        data = pkgutil.get_data('qless', 'qless-core/qless.lua')
        self._lua = self.redis.register_script(data)
        #self._lua = lua('qless')

    def __getattr__(self, key):
        if key == 'events':
            self.events = Events(self)
            return self.events
        raise AttributeError('%s has no attribute %s' % (
            self.__class__.__module__ + '.' + self.__class__.__name__, key))

    def __call__(self, command, *args):
        lua_args = [command, repr(time.time())]
        lua_args.extend(args)
        try:
            return self._lua(keys=[], args=lua_args)
        except redis.ResponseError as exc:
            raise QlessException(exc.message)

    def track(self, jid):
        '''Begin tracking this job'''
        return self('track', 'track', jid)

    def untrack(self, jid):
        '''Stop tracking this job'''
        return self('track', 'untrack', jid)

    def tags(self, offset=0, count=100):
        '''The most common tags among jobs'''
        return json.loads(self('tag', 'top', offset, count))

    def event(self):
        '''Listen for a single event'''
        pass

    def listen(self, *args, **kwargs):
        '''Listen indefinitely for all events'''
        while True:
            self.event(*args, **kwargs)

    def unfail(self, group, queue, count=500):
        '''Move jobs from the failed group to the provided queue'''
        return self('unfail', queue, group, count)

from .lua import lua
from .job import Job, RecurringJob
from .queue import Queue
from .config import Config
