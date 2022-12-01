# schema.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Dictionaries for Flow-By-Activity and Flow-By-Sector datasets
and their variations
"""

flow_by_activity_fields = \
    {'Class': [{'dtype': 'str'}, {'required': True}],
     'SourceName': [{'dtype': 'str'}, {'required': True}],
     'FlowName': [{'dtype': 'str'}, {'required': True}],
     'FlowAmount': [{'dtype': 'float'}, {'required': True}],
     'Unit': [{'dtype': 'str'}, {'required': True}],
     'FlowType': [{'dtype': 'str'}, {'required': True}],
     'ActivityProducedBy': [{'dtype': 'str'}, {'required': False}],
     'ActivityConsumedBy': [{'dtype': 'str'}, {'required': False}],
     'Compartment': [{'dtype': 'str'}, {'required': False}],
     'Location': [{'dtype': 'str'}, {'required': True}],
     'LocationSystem': [{'dtype': 'str'}, {'required': True}],
     'Year': [{'dtype': 'int'}, {'required': True}],
     'MeasureofSpread': [{'dtype': 'str'}, {'required': False}],
     'Spread': [{'dtype': 'float'}, {'required': False}],
     'DistributionType': [{'dtype': 'str'}, {'required': False}],
     'Min': [{'dtype': 'float'}, {'required': False}],
     'Max': [{'dtype': 'float'}, {'required': False}],
     'DataReliability': [{'dtype': 'float'}, {'required': True}],
     'DataCollection': [{'dtype': 'float'}, {'required': True}],
     'Description': [{'dtype': 'str'}, {'required': True}]
     }
flow_by_sector_fields = \
    {'Flowable': [{'dtype': 'str'}, {'required': True}],
     'Class': [{'dtype': 'str'}, {'required': True}],
     'SectorProducedBy': [{'dtype': 'str'}, {'required': False}],
     'SectorConsumedBy': [{'dtype': 'str'}, {'required': False}],
     'SectorSourceName': [{'dtype': 'str'}, {'required': False}],
     'Context': [{'dtype': 'str'}, {'required': True}],
     'Location': [{'dtype': 'str'}, {'required': True}],
     'LocationSystem': [{'dtype': 'str'}, {'required': True}],
     'FlowAmount': [{'dtype': 'float'}, {'required': True}],
     'Unit': [{'dtype': 'str'}, {'required': True}],
     'FlowType': [{'dtype': 'str'}, {'required': True}],
     'Year': [{'dtype': 'int'}, {'required': True}],
     'ProducedBySectorType': [{'dtype': 'str'}, {'required': False}],
     'ConsumedBySectorType': [{'dtype': 'str'}, {'required': False}],
     'MeasureofSpread': [{'dtype': 'str'}, {'required': False}],
     'Spread': [{'dtype': 'float'}, {'required': False}],
     'DistributionType': [{'dtype': 'str'}, {'required': False}],
     'Min': [{'dtype': 'float'}, {'required': False}],
     'Max': [{'dtype': 'float'}, {'required': False}],
     'DataReliability': [{'dtype': 'float'}, {'required': True}],
     'TemporalCorrelation': [{'dtype': 'float'}, {'required': True}],
     'GeographicalCorrelation': [{'dtype': 'float'}, {'required': True}],
     'TechnologicalCorrelation': [{'dtype': 'float'}, {'required': True}],
     'DataCollection': [{'dtype': 'float'}, {'required': True}],
     'MetaSources': [{'dtype': 'str'}, {'required': True}],
     'FlowUUID': [{'dtype': 'str'}, {'required': True}]
     }
flow_by_sector_fields_w_activity = flow_by_sector_fields.copy()
flow_by_sector_fields_w_activity.update(
 {'ActivityProducedBy': [{'dtype': 'str'}, {'required': False}],
  'ActivityConsumedBy': [{'dtype': 'str'}, {'required': False}]})
flow_by_sector_collapsed_fields = \
    {'Flowable': [{'dtype': 'str'}, {'required': True}],
     'Class': [{'dtype': 'str'}, {'required': True}],
     'Sector': [{'dtype': 'str'}, {'required': False}],
     'SectorSourceName': [{'dtype': 'str'}, {'required': False}],
     'Context': [{'dtype': 'str'}, {'required': True}],
     'Location': [{'dtype': 'str'}, {'required': True}],
     'LocationSystem': [{'dtype': 'str'}, {'required': True}],
     'FlowAmount': [{'dtype': 'float'}, {'required': True}],
     'Unit': [{'dtype': 'str'}, {'required': True}],
     'FlowType': [{'dtype': 'str'}, {'required': True}],
     'Year': [{'dtype': 'int'}, {'required': True}],
     'MeasureofSpread': [{'dtype': 'str'}, {'required': False}],
     'Spread': [{'dtype': 'float'}, {'required': False}],
     'DistributionType': [{'dtype': 'str'}, {'required': False}],
     'Min': [{'dtype': 'float'}, {'required': False}],
     'Max': [{'dtype': 'float'}, {'required': False}],
     'DataReliability': [{'dtype': 'float'}, {'required': True}],
     'TemporalCorrelation': [{'dtype': 'float'}, {'required': True}],
     'GeographicalCorrelation': [{'dtype': 'float'}, {'required': True}],
     'TechnologicalCorrelation': [{'dtype': 'float'}, {'required': True}],
     'DataCollection': [{'dtype': 'float'}, {'required': True}],
     'MetaSources': [{'dtype': 'str'}, {'required': True}],
     'FlowUUID': [{'dtype': 'str'}, {'required': True}]
     }
flow_by_activity_mapped_fields = \
    {'Class': [{'dtype': 'str'}, {'required': True}],
     'SourceName': [{'dtype': 'str'}, {'required': True}],
     'FlowName': [{'dtype': 'str'}, {'required': True}],
     'Flowable': [{'dtype': 'str'}, {'required': True}],
     'FlowAmount': [{'dtype': 'float'}, {'required': True}],
     'Unit': [{'dtype': 'str'}, {'required': True}],
     'FlowType': [{'dtype': 'str'}, {'required': True}],
     'ActivityProducedBy': [{'dtype': 'str'}, {'required': False}],
     'ActivityConsumedBy': [{'dtype': 'str'}, {'required': False}],
     'Compartment': [{'dtype': 'str'}, {'required': False}],
     'Context': [{'dtype': 'str'}, {'required': False}],
     'Location': [{'dtype': 'str'}, {'required': True}],
     'LocationSystem': [{'dtype': 'str'}, {'required': True}],
     'Year': [{'dtype': 'int'}, {'required': True}],
     'MeasureofSpread': [{'dtype': 'str'}, {'required': False}],
     'Spread': [{'dtype': 'float'}, {'required': False}],
     'DistributionType': [{'dtype': 'str'}, {'required': False}],
     'Min': [{'dtype': 'float'}, {'required': False}],
     'Max': [{'dtype': 'float'}, {'required': False}],
     'DataReliability': [{'dtype': 'float'}, {'required': True}],
     'DataCollection': [{'dtype': 'float'}, {'required': True}],
     'Description': [{'dtype': 'str'}, {'required': True}],
     'FlowUUID': [{'dtype': 'str'}, {'required': True}]
     }
flow_by_activity_wsec_fields = \
    {'Class': [{'dtype': 'str'}, {'required': True}],
     'SourceName': [{'dtype': 'str'}, {'required': True}],
     'FlowName': [{'dtype': 'str'}, {'required': True}],
     'FlowAmount': [{'dtype': 'float'}, {'required': True}],
     'Unit': [{'dtype': 'str'}, {'required': True}],
     'FlowType': [{'dtype': 'str'}, {'required': True}],
     'ActivityProducedBy': [{'dtype': 'str'}, {'required': False}],
     'ActivityConsumedBy': [{'dtype': 'str'}, {'required': False}],
     'Compartment': [{'dtype': 'str'}, {'required': False}],
     'Location': [{'dtype': 'str'}, {'required': True}],
     'LocationSystem': [{'dtype': 'str'}, {'required': True}],
     'Year': [{'dtype': 'int'}, {'required': True}],
     'MeasureofSpread': [{'dtype': 'str'}, {'required': False}],
     'Spread': [{'dtype': 'float'}, {'required': False}],
     'DistributionType': [{'dtype': 'str'}, {'required': False}],
     'Min': [{'dtype': 'float'}, {'required': False}],
     'Max': [{'dtype': 'float'}, {'required': False}],
     'DataReliability': [{'dtype': 'float'}, {'required': True}],
     'DataCollection': [{'dtype': 'float'}, {'required': True}],
     'SectorProducedBy': [{'dtype': 'str'}, {'required': False}],
     'SectorConsumedBy': [{'dtype': 'str'}, {'required': False}],
     'SectorSourceName': [{'dtype': 'str'}, {'required': False}],
     'ProducedBySectorType': [{'dtype': 'str'}, {'required': False}],
     'ConsumedBySectorType': [{'dtype': 'str'}, {'required': False}]
     }
flow_by_activity_mapped_wsec_fields = \
    {'Class': [{'dtype': 'str'}, {'required': True}],
     'SourceName': [{'dtype': 'str'}, {'required': True}],
     'FlowName': [{'dtype': 'str'}, {'required': True}],
     'Flowable': [{'dtype': 'str'}, {'required': True}],
     'FlowAmount': [{'dtype': 'float'}, {'required': True}],
     'Unit': [{'dtype': 'str'}, {'required': True}],
     'FlowType': [{'dtype': 'str'}, {'required': True}],
     'ActivityProducedBy': [{'dtype': 'str'}, {'required': False}],
     'ActivityConsumedBy': [{'dtype': 'str'}, {'required': False}],
     'Compartment': [{'dtype': 'str'}, {'required': False}],
     'Context': [{'dtype': 'str'}, {'required': False}],
     'Location': [{'dtype': 'str'}, {'required': True}],
     'LocationSystem': [{'dtype': 'str'}, {'required': True}],
     'Year': [{'dtype': 'int'}, {'required': True}],
     'MeasureofSpread': [{'dtype': 'str'}, {'required': False}],
     'Spread': [{'dtype': 'float'}, {'required': False}],
     'DistributionType': [{'dtype': 'str'}, {'required': False}],
     'Min': [{'dtype': 'float'}, {'required': False}],
     'Max': [{'dtype': 'float'}, {'required': False}],
     'DataReliability': [{'dtype': 'float'}, {'required': True}],
     'DataCollection': [{'dtype': 'float'}, {'required': True}],
     'Description': [{'dtype': 'str'}, {'required': True}],
     'FlowUUID': [{'dtype': 'str'}, {'required': True}],
     'SectorProducedBy': [{'dtype': 'str'}, {'required': False}],
     'SectorConsumedBy': [{'dtype': 'str'}, {'required': False}],
     'SectorSourceName': [{'dtype': 'str'}, {'required': False}],
     'ProducedBySectorType': [{'dtype': 'str'}, {'required': False}],
     'ConsumedBySectorType': [{'dtype': 'str'}, {'required': False}]
     }
# A list of activity fields in each flow data format
activity_fields = {'ProducedBy': [{'flowbyactivity': 'ActivityProducedBy'},
                                  {'flowbysector': 'SectorProducedBy'}],
                   'ConsumedBy': [{'flowbyactivity': 'ActivityConsumedBy'},
                                  {'flowbysector': 'SectorConsumedBy'}]
                   }

dq_fields = ['MeasureofSpread', 'Spread', 'DistributionType', 'Min',
             'Max', 'DataReliability', 'DataCollection', 'TemporalCorrelation',
             'GeographicalCorrelation', 'TechnologicalCorrelation']
