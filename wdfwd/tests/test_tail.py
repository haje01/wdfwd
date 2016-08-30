import os
import glob
import time
import shutil

import pytest

from wdfwd.get_config import get_config
from wdfwd.tail import FileTailer, NoTargetFile, TailThread, get_file_lineinfo,\
    FluentCfg, KinesisCfg, MAX_READ_BUF
from wdfwd.util import InvalidLogFormat, KN_TEST_STREAM, iter_kinesis_records

cfg = get_config()

tcfg = cfg.get('tailing')
pos_dir = tcfg.get('pos_dir')
fluent = tcfg['to']['fluent']
fluent_ip = os.environ.get('WDFWD_TEST_FLUENT_IP')
fluent_port = int(os.environ.get('WDFWD_TEST_FLUENT_PORT', 0))
fcfg = FluentCfg(fluent_ip, fluent_port)


@pytest.fixture(scope='function')
def rmlogs():
    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']
    plogs = glob.glob(os.path.join(bdir, '*.log'))
    for plog in plogs:
        os.remove(plog)

    poss = glob.glob(os.path.join(pos_dir, '*.pos'))
    for pos in poss:
        os.remove(pos)


@pytest.fixture(scope='function')
def ftail():
    return _ftail()


@pytest.fixture(scope='function')
def ktail():
    return _ktail()


def _ftail():
    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']
    ptrn = finfo['pattern']
    tag = finfo['tag']
    tail = FileTailer(bdir, ptrn, tag, pos_dir, fcfg,
                      send_term=0, update_term=0, echo=True,
                      max_between_data=100 * 100)
    return tail


@pytest.fixture(scope='function')
def ftail_fmt():
    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']
    ptrn = finfo['pattern']
    tag = finfo['tag']
    fmt = '(?P<dt_>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'
    tail = FileTailer(bdir, ptrn, tag, pos_dir, fcfg,
                      send_term=0, update_term=0, echo=True,
                      max_between_data=100 * 100, format=fmt)
    return tail


def _ktail():
    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']
    ptrn = finfo['pattern']
    tag = finfo['tag']
    kcfg = KinesisCfg(KN_TEST_STREAM, 'ap-northeast-1', None, None)
    tail = FileTailer(bdir, ptrn, tag, pos_dir, kcfg,
                      send_term=0, update_term=0, echo=True,
                      max_between_data=100 * 100)
    return tail


@pytest.fixture(scope='function')
def ftail2():
    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']
    ptrn = "tailtest2_*-*-*.log"
    ftail2 = FileTailer(bdir, ptrn, 'wdfwd.tail2', pos_dir, fcfg, send_term=0,
                        update_term=0, echo=True, max_between_data=100*100)
    return ftail2


def test_tail_init():
    assert tcfg is not None
    assert pos_dir is not None
    assert fluent is not None
    assert fluent_ip is not None
    assert fluent_port != 0


def test_tail_file_basic(rmlogs, ftail):
    with pytest.raises(NoTargetFile):
        ftail.get_file_pos()
    with pytest.raises(NoTargetFile):
        ftail.may_send_newlines()

    path = os.path.join(ftail.bdir, 'tailtest_2016-03-30.log')
    with open(path, 'w') as f:
        f.write('1\n')

    ftail.update_target()

    assert ftail.target_path.endswith("tailtest_2016-03-30.log")
    assert ftail.target_fid
    assert ftail.get_file_pos() == 3
    # send previous data if it has moderate size
    assert ftail.get_sent_pos() == 0
    assert ftail.may_send_newlines() == 1

    with open(path, 'a') as f:
        f.write('2\n')
    # send new line
    assert ftail.get_file_pos() == 6
    assert ftail.may_send_newlines() == 1
    assert ftail.get_sent_pos() == 6

    # send again send nothing
    assert ftail.may_send_newlines() == 0

    with open(path, 'a') as f:
        f.write('3\n')
        f.write('4\n')

    assert ftail.get_file_pos() == 12
    assert ftail.may_send_newlines() == 2
    assert ftail.get_sent_pos() == 12
    # not changed, send nothing
    assert ftail.may_send_newlines() == 0

    # stress test
    with open(path, 'a') as f:
        for i in xrange(4, 1000):
            f.write('{}\n'.format(i))
    assert ftail.may_send_newlines() == 996


def test_tail_file_kinesis(rmlogs, ktail):
    assert ktail.kclient is not None
    path = os.path.join(ktail.bdir, 'tailtest_2016-03-30.log')
    with open(path, 'w') as f:
        f.write('1\n')

    ktail.update_target()

    assert ktail.target_path.endswith("tailtest_2016-03-30.log")
    assert ktail.target_fid
    assert ktail.get_file_pos() == 3
    # send previous data if it has moderate size
    assert ktail.get_sent_pos() == 0
    assert ktail.may_send_newlines() == 1
    knc = ktail.kclient
    #for rec in iter_kinesis_records(knc, ktail.ksent_shid, ktail.ksent_seqn):
        #assert 'ts_' in rec

    with open(path, 'a') as f:
        f.write('2\n')
    # send new line
    assert ktail.get_file_pos() == 6
    assert ktail.may_send_newlines() == 1
    assert ktail.get_sent_pos() == 6

    # send again send nothing
    assert ktail.may_send_newlines() == 0

    with open(path, 'a') as f:
        f.write('3\n')
        f.write('4\n')

    assert ktail.get_file_pos() == 12
    assert ktail.may_send_newlines() == 2
    assert ktail.get_sent_pos() == 12
    # not changed, send nothing
    assert ktail.may_send_newlines() == 0

    # stress test
    with open(path, 'a') as f:
        for i in xrange(4, 100):
            f.write('{}\n'.format(i))
    assert ktail.may_send_newlines() == 96

    #for rec in iter_kinesis_records(knc, ktail.ksent_shid, ktail.ksent_seqn):
        #assert 'ts_' in rec


def test_tail_file_rotate(rmlogs, ftail):
    for i in range(1, 30):
        path = os.path.join(ftail.bdir,
                            'tailtest_2016-03-{:02d}.log'.format(i))
        with open(path, 'w') as f:
            f.write('{}\n'.format(i))

    ftail.update_target()
    assert os.path.basename(ftail.target_path) == 'tailtest_2016-03-29.log'
    assert ftail.may_send_newlines() == 1

    # new file
    path = os.path.join(ftail.bdir, 'tailtest_2016-03-30.log')
    with open(path, 'w') as f:
        f.write('30\n')

    ftail.update_target()
    assert os.path.basename(ftail.target_path) == 'tailtest_2016-03-30.log'
    assert ftail.may_send_newlines() == 1


def test_tail_file_multi(rmlogs, ftail, ftail2):
    path = os.path.join(ftail.bdir, 'tailtest_2016-03-30.log')
    with open(path, 'w') as f:
        for i in range(10):
            f.write('A\n')
    ftail.update_target()

    path = os.path.join(ftail.bdir, 'tailtest2_2016-03-30.log')
    with open(path, 'w') as f:
        for i in range(10):
            f.write('B\n')
    ftail2.update_target()

    assert ftail.may_send_newlines() == 10
    assert ftail2.may_send_newlines() == 10


def test_tail_file_thread(rmlogs, ftail, ftail2):
    # existing file, should skip old lines
    path = os.path.join(ftail.bdir, 'tailtest_2016-03-30.log')
    with open(path, 'w') as f:
        for i in range(500):
            f.write("A"*40 + " {}\n".format(i))
            if i % 2 == 0:
                f.flush()

    trd = TailThread('trd1', ftail)
    ftail.ldebug("test", "msg")
    trd2 = TailThread('trd2', ftail2)
    trd.start()
    trd2.start()

    time.sleep(2)

    with open(path, 'a') as f:
        path2 = os.path.join(ftail.bdir, 'tailtest2_2016-03-30.log')
        with open(path2, 'w') as f2:
            for i in range(500):
                f.write("A"*40 + " {}\n".format(i+500))
                f2.write("B"*40 + " {}\n".format(i))
                time.sleep(0.01)
                if i % 2 == 0:
                    f.flush()
                    f2.flush()

    time.sleep(2)

    path = os.path.join(ftail.bdir, 'tailtest_2016-04-01.log')
    with open(path, 'w') as f:
        path2 = os.path.join(ftail.bdir, 'tailtest2_2016-04-01.log')
        with open(path2, 'w') as f2:
            for i in range(500):
                f.write("A"*40 + "2 {}\n".format(i))
                f2.write("B"*40 + "2 {}\n".format(i))
                time.sleep(0.01)
                if i % 2 == 0:
                    f.flush()
                    f2.flush()

    time.sleep(3)
    trd.exit()
    trd2.exit()

    # check finally
    echo1 = ftail.echo_file.getvalue().splitlines()
    echo2 = ftail2.echo_file.getvalue().splitlines()

    # should skip previous data (1~500) larger than MAX_BETWEEN_DATA
    assert echo1[0].endswith('A 500')
    assert echo1[-1].endswith('A2 499')
    assert len(echo1) == 1000

    assert echo2[0].endswith('B 0')
    assert echo2[-1].endswith('B2 499')
    assert len(echo2) == 1000

    # sleep to prevent race condition among tests
    time.sleep(1)


@pytest.mark.skip(reason="remove skip mark to test service")
def test_tail_file_svc1(rmlogs):
    """
    service test for no elatest
    """
    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']

    path = os.path.join(bdir, 'tailtest_2016-04-01.log')
    with open(path, 'a') as f:
        path2 = os.path.join(bdir, 'tailtest2_2016-04-01.log')
        with open(path2, 'w') as f2:
            for i in range(100):
                f.write("A"*40 + " {}\n".format(i))
                f2.write("B"*40 + " {}\n".format(i))
                time.sleep(0.1)
                if i % 2 == 0:
                    f.flush()
                    f2.flush()
    print 'starting 2...'

    path = os.path.join(bdir, 'tailtest_2016-04-02.log')
    with open(path, 'a') as f:
        path2 = os.path.join(bdir, 'tailtest2_2016-04-02.log')
        with open(path2, 'w') as f2:
            for i in range(100):
                f.write("C"*40 + " {}\n".format(i))
                f2.write("D"*40 + " {}\n".format(i))
                time.sleep(0.1)
                if i % 2 == 0:
                    f.flush()
                    f2.flush()
    print 'waiting...'
    time.sleep(20)


@pytest.mark.skip(reason="remove skip mark to test service")
def test_tail_file_svc2(rmlogs):
    """
    service test for elatest
    """
    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']

    epath = os.path.join(bdir, 'exp_latest3.log')
    with open(epath, 'w') as f:
        for i in range(100):
            f.write("a"*40 + " {}\n".format(i))
            time.sleep(0.1)
            if i % 2 == 0:
                f.flush()

    print 'rotating latest...'
    pre_path = os.path.join(bdir, 'tailtest3_2016-03-30.log')
    shutil.move(epath, pre_path)

    with open(epath, 'w') as f:
        for i in range(100):
            f.write("b"*40 + " {}\n".format(i))
            time.sleep(0.1)
            if i % 2 == 0:
                f.flush()
    print 'waiting...'
    time.sleep(20)


@pytest.mark.skip(reason="remove skip mark to test service")
def test_tail_file_svc3(rmlogs):
    """
    service test for no elatest
    """
    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']

    data="""2016-03-30 16:10:50.5503 INFO: {"obj":[{"Delegate":{},"target0":{"returnObj":{"FirstItems":[{"ProductDisplaySeq":388}],"ProductDisplaySeq":389,"SecondItems":[{"ParentSeq":388,"ProductDisplaySeq":389},{"ParentSeq":388,"ProductDisplaySeq":461}],"ThirdItems":[],"Message":null,"Return":true,"ReturnCode":0,"TraceId":"6c8b7c6c-6c6a-4fcc-b879-72a64a4e57e5"},"parentSeq":0,"salesZone":421,"userSeq":0,"accountID":"","clientIp":"10.1.18.22"}}]}"""
    path = os.path.join(bdir, 'tailtest4_2016-03-30.log')
    with open(path, 'a') as f:
        f.write(data)
        time.sleep(0.1)
        f.flush()

    print 'waiting...'
    time.sleep(10)


@pytest.mark.skip(reason="remove skip mark to test service")
def test_tail_file_svc4(rmlogs):
    """
    service test for elatest
    """
    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']

    epath = os.path.join(bdir, 'exp_latest5.log')
    with open(epath, 'w') as f:
        for i in range(100):
            f.write("a"*40 + " {}\n".format(i))
            time.sleep(0.1)
            if i % 2 == 0:
                f.flush()

    print 'rotating latest...'
    pre_path = os.path.join(bdir, 'tailtest5_2016-03-30.9.log')
    shutil.move(epath, pre_path)

    print 'start new latest'
    with open(epath, 'w') as f:
        for i in range(100):
            f.write("b"*40 + " {}\n".format(i))
            time.sleep(0.1)
            if i % 2 == 0:
                f.flush()

    print 'rotating latest...'
    pre_path = os.path.join(bdir, 'tailtest5_2016-03-30.10.log')
    shutil.move(epath, pre_path)

    print 'waiting...'
    time.sleep(10)


@pytest.mark.skip(reason="remove skip mark to test service")
def test_tail_file_svc5(rmlogs):
    """
        Service test for parser
    """
    finfo = tcfg['from'][3]['file']
    bdir = finfo['dir']

    epath = os.path.join(bdir, 'format_test-2016-06-27.log')
    with open(epath, 'w') as f:
        for i in range(100):
            f.write('2016-06-27 09:10:00 DEBUG Message {}\n'.format(i))
            time.sleep(0.1)
            f.write('2016-06-27 09:10:10 DEBUG foo.py:171 Message {}\n'.format(i))
            time.sleep(0.1)


def test_tail_file_elatest1(rmlogs):
    """
    CASE 1: rotate elatest, process and new elatest
    """

    EXP_LATEST = 'exp_latest.log'
    PRE_LATEST = 'tailtest3_2016-03-30.9.log'
    PRE_LATEST2 = 'tailtest3_2016-03-30.10.log'

    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']

    epath = os.path.join(bdir, EXP_LATEST)
    with open(epath, 'w') as f:
        f.write('A\n')

    ptrn = "tailtest3_*-*-*.log"
    ftail = FileTailer(bdir, ptrn, 'wdfwd.tail', pos_dir, fcfg, 0,
                       elatest=EXP_LATEST,
                       order_ptrn=r'(?P<date>[^\.]+)\.(?P<order>\d+)\.log')
    ftail.update_target()
    rotated = ftail.handle_elatest_rotation()
    assert not rotated
    epath, efid = ftail.get_elatest_info()
    assert epath and efid
    assert ftail.target_path.endswith(EXP_LATEST)
    assert ftail.elatest_fid is not None
    # update target again
    ftail.update_target()
    assert ftail.target_path.endswith(EXP_LATEST)
    assert ftail.may_send_newlines() == 1
    elatest_sent_pos = ftail.get_sent_pos()

    with open(epath, 'a') as f:
        f.write('B\n')
        f.write('C\n')

    ### CASE 1: rotate elatest, process and new elatest
    ## rotate latest
    pre_sent_pos = ftail.get_sent_pos()
    pre_path = os.path.join(bdir, PRE_LATEST)
    shutil.move(epath, pre_path)
    assert os.path.isfile(pre_path)
    assert not os.path.isfile(epath)

    ## check elatest rotation
    rotated = ftail.handle_elatest_rotation(cur=0)
    assert rotated
    # now elatest fid is None
    assert ftail.elatest_fid is None
    ftail.update_target()
    # pre-elatest file is target
    assert ftail.target_path == pre_path
    # pre-elatest sent_pos is equal to elatest's
    assert ftail.get_sent_pos() == elatest_sent_pos
    # elatest sent_pos is cleared
    assert ftail.get_sent_pos(epath) == 0
    # send remain 2 lines
    assert ftail.may_send_newlines() == 2

    # new elatest
    with open(epath, 'w') as f:
        f.write('a\n')
        f.write('b\n')
        f.write('c\n')

    rotated = ftail.handle_elatest_rotation(cur=0)
    assert rotated == False
    ftail.update_target()
    assert ftail.target_path.endswith(EXP_LATEST)
    epath, efid = ftail.get_elatest_info()
    assert epath and efid
    assert ftail.get_sent_pos() == 0
    assert ftail.may_send_newlines() == 3

    # rotate again
    time.sleep(2)
    pre_path2 = os.path.join(bdir, PRE_LATEST2)
    shutil.move(epath, pre_path2)
    rotated = ftail.handle_elatest_rotation(cur=0)
    assert rotated
    ftail.update_target()
    assert ftail.target_path == pre_path2


def test_tail_file_elatest2(rmlogs):
    """
    CASE 2: rotate elatest, new elatest, update and process
    """
    import shutil

    EXP_LATEST = 'exp_latest.log'
    PRE_LATEST = 'tailtest3_2016-03-30.log'

    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']

    epath = os.path.join(bdir, EXP_LATEST)
    with open(epath, 'w') as f:
        f.write('A\n')

    ptrn = "tailtest3_*-*-*.log"
    ftail = FileTailer(bdir, ptrn, 'wdfwd.tail', pos_dir, fcfg, 0,
                       elatest=EXP_LATEST)
    ftail.update_target()
    rotated, psent_pos, sent_line = ftail.tmain()

    assert ftail.target_path.endswith(EXP_LATEST)
    assert ftail.elatest_fid is not None
    assert sent_line == 1

    elatest_sent_pos = ftail.get_sent_pos()
    with open(epath, 'a') as f:
        f.write('B\n')
        f.write('C\n')

    # rotate latest
    pre_sent_pos = ftail.get_sent_pos()
    pre_path = os.path.join(bdir, PRE_LATEST)
    shutil.move(epath, pre_path)
    assert os.path.isfile(pre_path)
    assert not os.path.isfile(epath)

    # new elatest
    with open(epath, 'w') as f:
        f.write('a\n')
        f.write('b\n')
        f.write('c\n')
    time.sleep(1)

    rotated, psent_pos, sent_line = ftail.tmain()
    assert rotated
    # pre-elatest file is target
    assert ftail.target_path == pre_path
    # pre-elatest sent_pos is equal to elatest's
    assert psent_pos == elatest_sent_pos
    # send remain 2 lines
    assert sent_line == 2

    # now new elatest is target
    ftail.update_target()
    assert ftail.target_path == epath
    assert ftail.may_send_newlines() == 3


def test_tail_file_elatest3(rmlogs):
    """
    CASE 3: rotate elatest, reversed pre-elatest order
    """
    import shutil

    EXP_LATEST = 'z1_action.log'
    PRE_LATEST = 'z1_action.log.1'
    PRE_LATEST2 = 'z1_action.log.2'

    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']

    epath = os.path.join(bdir, EXP_LATEST)
    with open(epath, 'w') as f:
        f.write('A\n')

    ptrn = "z*_action.*"
    ftail = FileTailer(bdir, ptrn, 'wdfwd.tail', pos_dir, fcfg, 0,
                       elatest=EXP_LATEST, reverse_order=True)
    ftail.update_target()
    rotated, psent_pos, sent_line = ftail.tmain()

    assert ftail.target_path.endswith(EXP_LATEST)
    assert ftail.elatest_fid is not None
    assert sent_line == 1

    elatest_sent_pos = ftail.get_sent_pos()
    with open(epath, 'a') as f:
        f.write('B\n')
        f.write('C\n')

    # rotate latest
    pre_sent_pos = ftail.get_sent_pos()
    pre_path = os.path.join(bdir, PRE_LATEST)
    shutil.move(epath, pre_path)
    assert os.path.isfile(pre_path)
    assert not os.path.isfile(epath)

    # new elatest
    with open(epath, 'w') as f:
        f.write('a\n')
        f.write('b\n')
        f.write('c\n')
    time.sleep(1)

    rotated, psent_pos, sent_line = ftail.tmain()
    assert rotated
    # pre-elatest file is target
    assert ftail.target_path == pre_path
    # pre-elatest sent_pos is equal to elatest's
    assert psent_pos == elatest_sent_pos
    # send remain 2 lines
    assert sent_line == 2

    # now new elatest is target
    ftail.update_target()
    assert ftail.target_path == epath
    assert ftail.may_send_newlines() == 3

    # rotate again
    time.sleep(2)
    pre_path2 = os.path.join(bdir, PRE_LATEST2)
    shutil.move(pre_path, pre_path2)
    shutil.move(epath, pre_path)
    rotated = ftail.handle_elatest_rotation(cur=0)
    assert rotated
    ftail.update_target()
    assert ftail.target_path == pre_path


def test_tail_file_continue(rmlogs, ftail):
    path = os.path.join(ftail.bdir, 'tailtest_2016-03-30.log')
    with open(path, 'w') as f:
        for i in range(100):
            f.write('{}\n'.format(i))

    ftail.update_target()
    assert ftail.may_send_newlines() == 100

    path = os.path.join(ftail.bdir, 'tailtest_2016-03-30.log')
    with open(path, 'a') as f:
        for i in range(100):
            f.write('{}\n'.format(i))

    # after reset, shall find target again and send previous data from sent pos
    ftail = _ftail()
    ftail.update_target()
    assert ftail.may_send_newlines() == 100
    echo = ftail.echo_file.getvalue().splitlines()
    assert len(echo) == 100


def test_tail_file_del(rmlogs, ftail):
    path = os.path.join(ftail.bdir, 'tailtest_2016-03-30.log')
    with open(path, 'w') as f:
        for i in range(100):
            f.write('{}\n'.format(i))

    ftail.update_target()
    assert ftail.target_path.endswith('tailtest_2016-03-30.log')
    pfile_id = ftail.target_fid
    assert pfile_id
    assert ftail.may_send_newlines() == 100
    os.remove(path)

    path = os.path.join(ftail.bdir, 'tailtest_2016-03-30.log')
    with open(path, 'w') as f:
        for i in range(10):
            f.write('{}\n'.format(i))

    ret = ftail.handle_file_recreate(0)
    assert ret == 2  # file recreated
    ftail.update_target()
    assert ftail.target_path.endswith('tailtest_2016-03-30.log')
    assert ftail.target_fid != pfile_id
    assert ftail.get_sent_pos() == 0
    assert ftail.may_send_newlines() == 10


def test_tail_file_startline(rmlogs):
    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']
    ptrn = finfo['pattern']
    tag = finfo['tag']

    path = os.path.join(bdir, 'tailtest_2016-03-30.log')
    with open(path, 'w') as f:
        for i in range(100):
            f.write('{}\n'.format(i))

    tail = FileTailer(bdir, ptrn, tag, pos_dir, fcfg, send_term=0,
                      update_term=0, echo=True, lines_on_start=0)

    tail.update_target(True)
    assert tail.may_send_newlines() == 100

    tail = FileTailer(bdir, ptrn, tag, pos_dir, fcfg, send_term=0,
                      update_term=0, echo=True, lines_on_start=10)

    tail.update_target(True)
    assert tail.may_send_newlines() == 10


def test_tail_file_lineinfo(rmlogs):
    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']

    path = os.path.join(bdir, 'tailtest_2016-03-30.log')
    with open(path, 'w') as f:
        for i in range(3):
            f.write('{}\n'.format(i))

    lines, pos = get_file_lineinfo(path, MAX_READ_BUF)
    assert lines == 3
    assert pos == 9

    lines, pos = get_file_lineinfo(path, MAX_READ_BUF, 1)
    assert lines == 2
    assert pos == 6

    with open(path) as f:
        f.seek(pos)
        assert f.read() == '2\n'

    lines, pos = get_file_lineinfo(path, MAX_READ_BUF, 3)
    assert lines == 0
    assert pos == 0

    lines, pos = get_file_lineinfo(path, MAX_READ_BUF, 4)
    assert lines == 0
    assert pos == 0


def test_tail_file_format(rmlogs):
    finfo = tcfg['from'][0]['file']
    bdir = finfo['dir']

    fmt = r'\s(?P<lvl>\S+):\s(?P<_json_>.+)'
    with pytest.raises(InvalidLogFormat):
        FileTailer(bdir, "tailtest4_*-*-*.log", "wdfwd.tail4", pos_dir, fcfg,
                   echo=True, format=fmt)

    fmt = r'(?P<dt_>\d+-\d+-\d+ \d+:\d+:\S+)\s(?P<lvl>\S+):\s(?P<_json_>.+)'
    tail = FileTailer(bdir, "tailtest4_*-*-*.log", "wdfwd.tail4", pos_dir,
                      fcfg, echo=True, format=fmt)
    data="""2016-03-30 16:10:50.5503 INFO: {"dt_": "to-be-overwritten", "obj":[{"Delegate":{},"target0":{"returnObj":{"FirstItems":[{"ProductDisplaySeq":388}],"ProductDisplaySeq":389,"SecondItems":[{"ParentSeq":388,"ProductDisplaySeq":389},{"ParentSeq":388,"ProductDisplaySeq":461}],"ThirdItems":[],"Message":null,"Return":true,"ReturnCode":0,"TraceId":"6c8b7c6c-6c6a-4fcc-b879-72a64a4e57e5"},"parentSeq":0,"salesZone":421,"userSeq":0,"accountID":"","clientIp":"10.1.18.22"}}]}"""
    path = os.path.join(bdir, 'tailtest4_2016-03-30.log')
    with open(path, 'w') as f:
        f.write(data)

    tail.update_target(True)
    assert tail.may_send_newlines() == 1
    echo = tail.echo_file.getvalue()
    assert 'dt_' in echo
    assert 'sname_' in echo
    assert 'saddr_' in echo
    assert 'lvl' in echo

    fmt = r'(?P<dt_>\d+-\d+-\d+ \d+:\d+:\S+)\s(?P<lvl>\S+):\s(?P<_text_>.+)'
    tail = FileTailer(bdir, "tailtest5_*-*-*.log", "wdfwd.tail4", pos_dir,
                      fcfg, echo=True, format=fmt)
    data="""2016-03-30 16:10:50.5503 INFO: Plain Text Message"""
    path = os.path.join(bdir, 'tailtest5_2016-03-30.log')
    with open(path, 'w') as f:
        f.write(data)

    tail.update_target(True)
    assert tail.may_send_newlines() == 1
    echo = tail.echo_file.getvalue()
    assert 'message' in echo
    assert 'dt' in echo
    assert 'lvl' in echo


def test_tail_iislog():
    data = '''
#Software: Microsoft Internet Information Services 7.5
#Version: 1.0
#Date: 2016-04-29 05:49:30
#Fields: date time s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs(User-Agent) sc-status sc-substatus sc-win32-status time-taken
2016-04-29 05:49:30 218.234.76.104 GET / - 80 - 10.1.18.22 Mozilla/5.0+(Windows+NT+6.1;+WOW64;+Trident/7.0;+rv:11.0)+like+Gecko 500 0 0 16891
2016-04-29 05:55:14 218.234.76.104 GET / - 80 - 10.1.18.40 Mozilla/5.0+(Windows+NT+6.3;+WOW64)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/50.0.2661.94+Safari/537.36 200 0 0 2312
2016-04-29 05:55:14 218.234.76.104 GET /Scripts/jquery.icheck.min.js - 80 - 10.1.18.40 Mozilla/5.0+(Windows+NT+6.3;+WOW64)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/50.0.2661.94+Safari/537.36 200 0 0 15
2016-04-29 05:55:14 218.234.76.104 GET /Scripts/jquery.DOMWindow.js - 80 - 10.1.18.40 Mozilla/5.0+(Windows+NT+6.3;+WOW64)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/50.0.2661.94+Safari/537.36 200 0 0 62
2016-04-29 05:55:14 218.234.76.104 GET /Scripts/select.js - 80 - 10.1.18.40 Mozilla/5.0+(Windows+NT+6.3;+WOW64)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/50.0.2661.94+Safari/537.36 200 0 0 62
2016-04-29 05:55:14 218.234.76.104 GET /n_portal/event/20150408_origin/floating_close.png - 80 - 10.1.18.40 Mozilla/5.0+(Windows+NT+6.3;+WOW64)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/50.0.2661.94+Safari/537.36 404 0 2 0
2016-04-29 05:59:54 218.234.76.104 GET / - 80 - 10.1.18.40 Mozilla/5.0+(Windows+NT+6.3;+WOW64)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/50.0.2661.94+Safari/537.36 200 0 0 15
2016-04-29 05:59:54 218.234.76.104 GET /n_portal/event/20150408_origin/floating_close.png - 80 - 10.1.18.40 Mozilla/5.0+(Windows+NT+6.3;+WOW64)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/50.0.2661.94+Safari/537.36 404 0 2 0
2016-04-29 06:00:39 218.234.76.104 GET / - 80 - 10.1.18.40 Mozilla/5.0+(Windows+NT+6.3;+WOW64)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/50.0.2661.94+Safari/537.36 200 0 0 15
2016-04-29 06:00:39 218.234.76.104 GET /n_portal/event/20150408_origin/floating_close.png - 80 - 10.1.18.40 Mozilla/5.0+(Windows+NT+6.3;+WOW64)+AppleWebKit/537.36+(KHTML,+like+Gecko)+Chrome/50.0.2661.94+Safari/537.36 404 0 2 0
'''
    import re
    fmt = r"(?P<dt_>\S+ \S+) (?P<s_ip>\S+) (?P<cs_method>\S+) (?P<cs_uri_stem>\S+) (?P<cs_uri_query>\S+) (?P<s_port>\S+) (?P<cs_username>\S+) (?P<c_ip>\S+) (?P<cs_useragent>\S+) (?P<sc_status>\S+) (?P<sc_substatus>\S+) (?P<sc_win32_status>\S+) (?P<time_taken>\S+)"
    ptrn = re.compile(fmt)
    for line in data.splitlines():
        if not line:
            continue
        match = ptrn.search(line)
        if not match:
            continue
        print match.groupdict()


@pytest.mark.skip(reason="not implemented. remove skip mark to develop")
def test_tail_uncompleted(rmlogs, ftail_fmt):
    path = os.path.join(ftail_fmt.bdir, 'tailtest_2016-03-30.log')
    with open(path, 'w') as f:
        f.write('2016-08-23 11:22:30\n')
        f.write('2016-08-23')
        f.flush()

    ftail_fmt.update_target()

    assert ftail_fmt.target_path.endswith("tailtest_2016-03-30.log")
    assert ftail_fmt.target_fid
    assert ftail_fmt.get_file_pos() == 31
    # send previous data if it has moderate size
    assert ftail_fmt.get_sent_pos() == 0
    assert ftail_fmt.may_send_newlines() == 1
    assert ftail_fmt.get_sent_pos() == 21

    with open(path, 'a') as f:
        f.write('11:22:33\n')
        f.flush()

    # send new line
    assert ftail_fmt.get_file_pos() == 42
    assert ftail_fmt.may_send_newlines() == 1
    assert ftail_fmt.get_sent_pos() == 42
