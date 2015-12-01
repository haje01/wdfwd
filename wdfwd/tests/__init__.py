def write_eloa_cfg(dcfg):
    import copy
    dcfg2 = copy.deepcopy(dcfg)
    dbc = dcfg2['db']
    dbc['connect']['database'] = 'EloaTest'
    dbct = dbc['table']
    dbct['names'] = ['CharStateLog_TBL', 'ChatingLog_TBL']
    dbct['date_column'] = 'LogTime'
    del dbct['date_pattern']
    del dbct['date_format']
    return dcfg2
