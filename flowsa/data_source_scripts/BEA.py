# BEA.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Supporting functions for BEA data.

Generation of BEA Gross Output data and industry transcation data as FBA,
Source csv files for BEA data are documented
in scripts/write_BEA_Use_from_useeior.py
"""
from functools import reduce
import pandas as pd
from flowsa.location import US_FIPS
from flowsa.common import fbs_activity_fields
from flowsa.schema import activity_fields
from flowsa.settings import externaldatapath
from flowsa.flowbyfunctions import assign_fips_location_system, aggregator
from flowsa.fbs_allocation import allocation_helper


def bea_gdp_parse(*, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # Read directly into a pandas df
    df_raw = pd.read_csv(externaldatapath + "BEA_GDP_GrossOutput_IO.csv")

    df = df_raw.rename(columns={'Unnamed: 0': 'ActivityProducedBy'})

    # use "melt" fxn to convert colummns into rows
    df = df.melt(id_vars=["ActivityProducedBy"],
                 var_name="Year",
                 value_name="FlowAmount")

    df = df[df['Year'] == year]
    # hardcode data
    df["Class"] = "Money"
    df["FlowType"] = "TECHNOSPHERE_FLOW"
    df['Description'] = 'BEA_2012_Detail_Code'
    df['FlowName'] = 'Gross Output'
    df["SourceName"] = "BEA_GDP_GrossOutput"
    df["Location"] = US_FIPS
    # original unit in million USD
    df['FlowAmount'] = df['FlowAmount'] * 1000000
    # state FIPS codes have not changed over last decade
    df['LocationSystem'] = "FIPS_2015"
    df["Unit"] = "USD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df


def bea_use_detail_br_parse(*, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param year: year)
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    csv_load = f'{externaldatapath}BEA_{str(year)}' \
               f'_Detail_Use_PRO_BeforeRedef.csv'
    df_raw = pd.read_csv(csv_load)

    df = bea_detail_parse(df_raw, year)
    df["SourceName"] = "BEA_Use_Detail_PRO_BeforeRedef"

    return df


def bea_make_detail_br_parse(*, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param year: year
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # Read directly into a pandas df
    csv_load = f'{externaldatapath}BEA_{str(year)}' \
               f'_Detail_Make_BeforeRedef.csv'
    df_raw = pd.read_csv(csv_load)

    df = bea_detail_parse(df_raw, year)
    df["SourceName"] = "BEA_Make_Detail_BeforeRedef"

    return df


def bea_detail_parse(df_raw, year):
    df = df_raw.rename(columns={'Unnamed: 0': 'ActivityProducedBy'})

    # use "melt" fxn to convert colummns into rows
    df = df.melt(id_vars=["ActivityProducedBy"],
                 var_name="ActivityConsumedBy",
                 value_name="FlowAmount")

    df['Year'] = str(year)
    # hardcode data
    df['FlowName'] = f"USD{str(year)}"
    df["Class"] = "Money"
    df["FlowType"] = "TECHNOSPHERE_FLOW"
    df['Description'] = 'BEA_2012_Detail_Code'
    df["Location"] = US_FIPS
    df['LocationSystem'] = "FIPS_2015"
    # original unit in million USD
    df['FlowAmount'] = df['FlowAmount'] * 1000000
    df["Unit"] = "USD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp
    return df


def bea_make_ar_parse(*, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param dataframe_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to
        flowbyactivity specifications
    """
    # df = pd.concat(dataframe_list, sort=False)
    df_load = pd.read_csv(externaldatapath + "BEA_" + year +
                          "_Make_AfterRedef.csv", dtype="str")
    # strip whitespace
    df = df_load.apply(lambda x: x.str.strip())
    # drop rows of data
    df = df[df['Industry'] == df['Commodity']].reset_index(drop=True)
    # drop columns
    df = df.drop(columns=['Commodity', 'CommodityDescription'])
    # rename columns
    df = df.rename(columns={'Industry': 'ActivityProducedBy',
                            'IndustryDescription': 'Description',
                            'ProVal': 'FlowAmount',
                            'IOYear': 'Year'})
    df.loc[:, 'FlowAmount'] = df['FlowAmount'].astype(float) * 1000000
    # hard code data
    df['Class'] = 'Money'
    df['SourceName'] = 'BEA_Make_AR'
    df['Unit'] = 'USD'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, year)
    df['FlowName'] = 'Gross Output Producer Value After Redef'
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp

    return df


def subset_BEA_table(df_load, attr, **_):
    """
    Modify loaded BEA table (make or use) based on data in the FBA method yaml
    :param df_load: df, flowbyactivity format
    :param attr: dictionary, attribute data from method yaml for activity set
    :return: modified BEA dataframe
    """
    df2 = pd.DataFrame()
    # extract commodity to filter and which Activity column used to filter
    for commodity, ActivityCol in attr['clean_parameter'].items():
        df = df_load.loc[df_load[ActivityCol] == commodity].reset_index(
            drop=True)

        # set column to None to enable generalizing activity column later
        df.loc[:, ActivityCol] = None
        if set(fbs_activity_fields).issubset(df.columns):
            for v in activity_fields.values():
                if v[0]['flowbyactivity'] == ActivityCol:
                    SectorCol = v[1]['flowbysector']
            df.loc[:, SectorCol] = None
        df2 = pd.concat([df, df2])

    # aggregate cols
    df3 = aggregator(df2, list(df2.select_dtypes(include=['object',
                                                          'int']).columns))

    return df3


def subset_and_allocate_BEA_table(df, attr, **_):
    """
    Temporary function to mimic use of 2nd helper allocation dataset
    """

    df = subset_BEA_table(df, attr)
    v = {'geoscale_to_use': 'national'}
    method2 = {'target_sector_source': 'NAICS_2012_Code'}

    import importlib
    fxn = getattr(importlib.import_module(
        'flowsa.data_source_scripts.BLS_QCEW'),
        "bls_clean_allocation_fba_w_sec")

    attr2 = {"helper_source": "BLS_QCEW",
             "helper_method": "proportional",
             "helper_source_class": "Employment",
             "helper_source_year": 2012,
             "helper_flow": ["Number of employees, Federal Government",
                             "Number of employees, State Government",
                             "Number of employees, Local Government",
                             "Number of employees, Private"],
             "helper_from_scale": "national",
             "allocation_from_scale": "national",
             "clean_helper_fba_wsec": fxn}
    df2 = allocation_helper(df, attr2, method2, v, False)
    # Drop remaining rows with no sectors e.g. T001 and other final demands
    df2 = df2.dropna(subset=['SectorConsumedBy']).reset_index(drop=True)
    return df2


def subset_and_equally_allocate_BEA_table(df, attr, **_):
    """
    Temporary function to equally attribute BEA table. This function will
    be unnecessary after merge with recursive branch
    """
    # Necessary to run equal attribution before subsetting dataset by an
    # activity because in some situations both the BEA ActivityConsumedBy
    # and ActivityProducedBy values map to multiple sectors. The
    # "subset_BEA_table()" fxn resets one of the activity columns to NaN.
    # For example, for "Liming" in the GHG attribution model,
    # the ActivityProducedBy 327500 maps to both 327410 and 327420 - so data
    # is double counted, but subsetting the BEA table resets
    # ActivityProducedBy to NaN and that double counting is lost.

    # equally attribute bea codes to each mapped sector
    groupby_cols = ['group_id']
    for c in ['Produced', 'Consumed']:
        df = (
            df
            .assign(
                **{f'_naics_{n}': df[f'Sector{c}By'].str.slice(stop=n)
                   for n in range(2, 7)},
                **{f'_unique_naics_{n}_by_group': lambda x, i=n: (
                    x.groupby(groupby_cols if i == 2
                              else [*groupby_cols, f'_naics_{i - 1}'],
                              dropna=False)
                    [[f'_naics_{i}']]
                    .transform('nunique', dropna=False)
                )
                   for n in range(2, 7)},
                FlowAmount=lambda x: reduce(
                    lambda x, y: x / y,
                    [x.FlowAmount, *[x[f'_unique_naics_{n}_by_group']
                                     for n in range(2, 7)]]
                )
            )
        )
        groupby_cols.append(f'Sector{c}By')

        df = df.drop(
            columns=[*[f'_naics_{n}' for n in range(2, 7)],
                     *[f'_unique_naics_{n}_by_group' for n in range(2, 7)]]
        )

    df2 = subset_BEA_table(df, attr)

    return df2
