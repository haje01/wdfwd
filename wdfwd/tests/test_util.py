

def test_util_ravel_dict():
    from wdfwd.util import ravel_dict
    data = dict(
        a=dict(
            b=0
        ),
        c=[1, 2, 3]
    )
    ret = ravel_dict(data)
    assert ret['a_b'] == 0
    assert ret['c'] == [1, 2, 3]
    assert 'a' not in ret
