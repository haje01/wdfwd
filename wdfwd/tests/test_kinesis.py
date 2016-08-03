import time
import uuid
import logging
from base64 import b64encode, b64decode

import pytest
import boto3
from aws_kinesis_agg import aggregator
from aws_kinesis_agg.deaggregator import deaggregate_records

from wdfwd.util import prepare_kinesis_test, aws_lambda_dform, KN_TEST_STREAM


DEL_STREAM_AFTER = False

# increase boto3 logging level
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('nose').setLevel(logging.WARNING)


@pytest.yield_fixture(scope='module')
def knc():
    knc = prepare_kinesis_test('ap-northeast-1')
    yield knc

    if DEL_STREAM_AFTER:
        knc.delete_stream(StreamName=KN_TEST_STREAM)


@pytest.mark.skip(reason="remove skip mark to test kinesis")
def test_kinesis_one(knc):
    st = time.time()
    seq = None
    n_send = 100
    data = "test data"
    for i in range(n_send):
        ret = knc.put_record(StreamName=KN_TEST_STREAM, Data=b64encode(data),
                            PartitionKey='pk', SequenceNumberForOrdering='0')
        assert 'ShardId' in ret
        assert ret['ResponseMetadata']['HTTPStatusCode'] == 200
        assert 'SequenceNumber' in ret
        if not seq:
            seq = ret['SequenceNumber']
    elapsed = time.time() - st
    speed = n_send / elapsed
    # put_record speed is over 35 records / sec
    assert speed > 35
    print("---------------- put_record: {} records/sec ----------------".format(speed))

    ret = knc.get_shard_iterator(
        StreamName=KN_TEST_STREAM,
        ShardId='shardId-000000000000',
        ShardIteratorType='AT_SEQUENCE_NUMBER',
        StartingSequenceNumber=seq
    )
    assert 'ShardIterator' in ret
    shdit = ret['ShardIterator']

    while True:
        ret = knc.get_records(ShardIterator=shdit)
        if len(ret['Records']) == 0:
            break
        assert 'Records' in ret
        assert 'NextShardIterator' in ret
        assert 'MillisBehindLatest' in ret
        shdit = ret['NextShardIterator']


def make_records(cnt, dict_rec=True):
    records = []
    data = "test data " * 12
    for i in range(cnt):
        pk = str(uuid.uuid4())
        ehk = str(uuid.uuid4().int)
        if dict_rec:
            rec = dict(Data="{} {}".format(i, data), PartitionKey=pk,
                    ExplicitHashKey=ehk)
            records.append(rec)
        else:
            records.append((pk, data, ehk))
    return records


@pytest.mark.skip(reason="remove skip mark to test kinesis")
def test_kinesis_bulk(knc):
    st = time.time()
    seq = None
    n_records = 100
    n_send = 100
    records = make_records(n_records)

    seq = None
    for i in range(n_send):
        ret = knc.put_records(Records=records, StreamName=KN_TEST_STREAM)
        assert len(ret['Records']) == n_records
        if ret['FailedRecordCount'] > 0:
            print("Record fail occurred at {}th put".format(i))
            break
        if not seq:
            seq = ret['Records'][0]['SequenceNumber']

    elapsed = time.time() - st
    speed = n_records / elapsed
    print("---------------- put_records: {} records/sec ----------------".format(speed))

    ret = knc.get_shard_iterator(
        StreamName=KN_TEST_STREAM,
        ShardId='shardId-000000000000',
        ShardIteratorType='AT_SEQUENCE_NUMBER',
        StartingSequenceNumber=seq
    )
    assert 'ShardIterator' in ret
    shdit = ret['ShardIterator']

    while True:
        ret = knc.get_records(ShardIterator=shdit)
        if len(ret['Records']) == 0:
            break
        assert 'Records' in ret
        assert 'NextShardIterator' in ret
        assert 'MillisBehindLatest' in ret
        shdit = ret['NextShardIterator']


#@pytest.mark.skip(reason="remove skip mark to test kinesis")
def test_kinesis_agg(knc):

    def put_record(knc, res, seq):
        pk, ehk, data = res.get_contents()
        ret = knc.put_record(StreamName=KN_TEST_STREAM,
                                Data=data,
                                PartitionKey=pk,
                                ExplicitHashKey=ehk)

        assert 'ShardId' in ret
        assert ret['ResponseMetadata']['HTTPStatusCode'] == 200
        if not seq:
            seq = ret['SequenceNumber']
        return seq

    st = time.time()
    seq = None
    n_records = 5000
    flush_size = 100
    records = make_records(n_records, False)

    agg = aggregator.RecordAggregator()

    seq = None
    for i, r in enumerate(records):
        res = agg.add_user_record(r[0], r[1],)
        if res or (i > 0 and i % flush_size == 0):
            print res, i
            res = agg.clear_and_get()
            print("Sending {} th aggregated records".format(i / flush_size + 1))
            seq = put_record(knc, res, seq)

    # send left records
    res = agg.clear_and_get()
    seq = put_record(knc, res, seq)

    elapsed = time.time() - st
    speed = n_records / elapsed
    print("---------------- agg put_record: {} records/sec ----------------".format(speed))

    #ret = knc.get_shard_iterator(
        #StreamName=KN_TEST_STREAM,
        #ShardId='shardId-000000000000',
        #ShardIteratorType='AT_SEQUENCE_NUMBER',
        #StartingSequenceNumber=seq
    #)
    #assert 'ShardIterator' in ret

    #shdit = ret['ShardIterator']

    #rcnt = 0
    #while True:
        #ret = knc.get_records(ShardIterator=shdit)
        #if len(ret['Records']) == 0:
            #break
        #assert 'Records' in ret
        ## padding error!?
        #records = deaggregate_records(aws_lambda_dform(ret['Records']))
        #import pdb; pdb.set_trace()  # XXX BREAKPOINT
        #for rec in records:
            #data = b64decode(rec['kinesis']['data'])
            #assert 'test data' in data
            #rcnt += 1

        #shdit = ret['NextShardIterator']

    #assert rcnt == n_records
