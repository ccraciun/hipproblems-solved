"""Distributed work queue test harness."""

# Enqueues work items in Redis, waits N seconds, and then tries to validate
# the result.

# The work items are json blobs of the form:
# {
#     'job_id': <JOB_ID>
#     'attempt_nr': <ATTEMPT_NR>
#     'value': <VALUE>
# }

# The expected result is the sum of all the values, grouped by job_id. Note,
# each run of workqueue_runner will only generate work for a single job_id, so
# workers can safely ignore that field when pulling work.

# Results are stored in a Redis hash called "hipmunk:result", that has 1
# field per job_id, where the value is the sum of all the work items with the
# corresponding job_id.


from gevent import monkey
monkey.patch_all()

import argparse
import collections
import gevent
import json
import logging
import random
import redis
import sets


def make_redis_key(key):
    return 'hipmunk:%s' % key


WORK_QUEUE = make_redis_key('queue')
RESULT_HASH = make_redis_key('result')
LOG = logging.getLogger(__name__)
REDIS = None

BATCH_SIZE = 20

aborted_transactions = [0]


def worker(worker_id):
    """
    Simpler worker implementation.

    Pulls work items from the queue, and adds their values to the result hash
    """
    while True:
        with REDIS.pipeline() as pipe:
            try:
                pipe.watch(WORK_QUEUE)
                work_raw = pipe.zrange(WORK_QUEUE, 0, BATCH_SIZE)
                if not work_raw:
                    return

                LOG.debug('Got raw work! worker_id: %d,  work: %r', worker_id, work_raw)
                batch_result = collections.defaultdict(int)
                for item_raw in work_raw:
                    work = json.loads(item_raw)
                    batch_result[work['job_id']] += work['value']
                LOG.debug('Work results: %r', batch_result)

                pipe.multi()
                pipe.zrem(WORK_QUEUE, *work_raw)
                for job_id, value in batch_result.items():
                    pipe.hincrby(RESULT_HASH, job_id, value)
                pipe.execute()
            except redis.exceptions.WatchError:
                aborted_transactions[0] += 1
                continue


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--num-work-items', '-n',
        type=int,
        help='number of work items to enqueue',
        default=10)
    parser.add_argument(
        '--num-workers', '-w',
        type=int,
        help='number of worker greenlets to spawn',
        default=1)
    parser.add_argument(
        '--timeout', '-t',
        type=int,
        help='time to wait for a result (in seconds)',
        default=5)
    parser.add_argument(
        '--loglevel', type=str, help='loglevel to use', default='INFO')
    parser.add_argument(
        '--host', help='Redis hostname', default='localhost')
    parser.add_argument(
        '--port', help='Redis port', type=int, default=6379)
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)-15s %(name)-12s: %(levelname)-8s %(message)s')
    LOG.setLevel(args.loglevel)

    REDIS = redis.StrictRedis(host=args.host, port=args.port, db=0)

    # Delete any old work keys (also serves to verify the Redis connection)
    try:
        REDIS.delete(WORK_QUEUE, RESULT_HASH)
    except redis.exceptions.ConnectionError:
        LOG.error(
            'Unable to connect to Redis at: %s:%d', args.host, args.port)
        exit(1)

    # Enqueue the work items
    job_id = random.randint(1, 100)
    unique_values = sets.Set()
    expected_result = 0
    LOG.info('Starting enqueueing')
    for _ in range(args.num_work_items):
        value = random.randint(1, 1000000)
        w = {
            'job_id': job_id,
            'attempt_nr': 1,
            'value': value,
        }
        score = random.random()
        if value not in unique_values:
            expected_result += value
            unique_values.add(value)
        LOG.debug("Enqueued with score %f work item: %r", score, w)
        REDIS.zadd(WORK_QUEUE, score, json.dumps(w))
    LOG.info('Done enqueueing')

    # Spawn workers, and wait for them to finish
    LOG.info('Starting workers')
    workers = [gevent.spawn(worker, n) for n in range(args.num_workers)]
    LOG.info('Waiting for work queue to drain.')
    while(REDIS.zcard(WORK_QUEUE) > 0):
        gevent.sleep(0.1)
    LOG.info('Work queue drained. Collecting workers.')
    gevent.joinall(workers, timeout=args.timeout)
    LOG.info('Collected all workers.')

    # Verify the result
    res = REDIS.hget(RESULT_HASH, job_id)
    if res is None:
        LOG.error('Unable to find the result in Redis.')
        exit(1)

    # Compare result to the expected value
    res = int(res)

    if res == expected_result:
        LOG.info('SUCCESS!')
    else:
        LOG.warn(
            "Result doesn't match expected value: %d vs %d",
            res,
            expected_result
        )
    LOG.info('Aborted redis transactions: %d', aborted_transactions[0])
