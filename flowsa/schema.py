# schema.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Dictionaries for Flow-By-Activity and Flow-By-Sector datasets
and their variations
"""
from flowsa import (settings, flowsa_yaml)


with open(settings.datapath + 'flowby_config.yaml') as f:
    flowby_config = flowsa_yaml.load(f)

def create_fill_na_dict(flow_by_fields):
    """
    Dictionary for how to fill nan in different column types
    :param flow_by_fields: list of columns
    :return: dictionary for how to fill missing values by dtype
    """
    fill_na_dict = {}
    for k, v in flow_by_fields.items():
        if v == 'str':
            fill_na_dict[k] = ""
        elif v in ['int', 'float']:
            fill_na_dict[k] = 0
    return fill_na_dict


def get_flow_by_groupby_cols(flow_by_fields):
    """
    Return groupby columns for a type of dataframe
    :param flow_by_fields: dictionary
    :return: list, column names
    """
    groupby_cols = []
    for k, v in flow_by_fields.items():
        if v in ['str', 'object', 'int']:
            groupby_cols.append(k)
    if flow_by_fields == flowby_config['fba_fields']:
        # Do not use description for grouping
        groupby_cols.remove('Description')
    return groupby_cols


fba_activity_fields = [
    flowby_config['activity_fields']['ProducedBy']['flowbyactivity'],
    flowby_config['activity_fields']['ConsumedBy']['flowbyactivity']
    ]
fbs_activity_fields = [
    flowby_config['activity_fields']['ProducedBy']['flowbysector'],
    flowby_config['activity_fields']['ConsumedBy']['flowbysector']
    ]
fba_fill_na_dict = create_fill_na_dict(flowby_config['fba_fields'])
fbs_fill_na_dict = create_fill_na_dict(flowby_config['fbs_fields'])
fbs_collapsed_fill_na_dict = create_fill_na_dict(
    flowby_config['fbs_collapsed_fields'])
fba_default_grouping_fields = get_flow_by_groupby_cols(
    flowby_config['fba_fields'])
fba_mapped_default_grouping_fields = get_flow_by_groupby_cols(
    flowby_config['fba_mapped_fields'])
fba_mapped_wsec_default_grouping_fields = get_flow_by_groupby_cols(
    flowby_config['fba_mapped_w_sector_fields'])
fbs_default_grouping_fields = get_flow_by_groupby_cols(
    flowby_config['fbs_fields'])
fbs_grouping_fields_w_activities = (
        fbs_default_grouping_fields + (['ActivityProducedBy',
                                        'ActivityConsumedBy']))
fbs_collapsed_default_grouping_fields = get_flow_by_groupby_cols(
    flowby_config['fbs_collapsed_fields'])
fba_wsec_default_grouping_fields = get_flow_by_groupby_cols(
    flowby_config['fba_w_sector_fields'])

## TODO update the names below over time to use flowby_config directly
flow_by_activity_fields = flowby_config['fba_fields']

flow_by_sector_fields = flowby_config['fbs_fields']

flow_by_sector_fields_w_activity = flowby_config['fbs_w_activity_fields']

flow_by_sector_collapsed_fields = flowby_config['fbs_collapsed_fields']

flow_by_activity_mapped_fields = flowby_config['fba_mapped_fields']

flow_by_activity_wsec_fields = flowby_config['fba_w_sector_fields']

flow_by_activity_mapped_wsec_fields = flowby_config['fba_mapped_w_sector_fields']

activity_fields = flowby_config['activity_fields']

dq_fields = flowby_config['dq_fields']
