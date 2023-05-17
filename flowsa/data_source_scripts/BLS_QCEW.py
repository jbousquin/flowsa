# BLS_QCEW.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Pulls Quarterly Census of Employment and Wages data in NAICS from Bureau
of Labor Statistics. Writes out to various FlowBySector class files for
these data items
EMP = Number of employees, Class = Employment
PAYANN = Annual payroll ($1,000), Class = Money
ESTAB = Number of establishments, Class = Other
This script is designed to run with a configuration parameter
--year = 'year' e.g. 2015
"""

import zipfile
import io
import pandas as pd
import numpy as np
from flowsa.location import US_FIPS
from flowsa.flowbyfunctions import assign_fips_location_system, \
    aggregator, equally_allocate_suppressed_parent_to_child_naics
from flowsa.flowby import FlowByActivity
from flowsa.flowsa_log import log
from flowsa.naics import industry_spec_key


def BLS_QCEW_URL_helper(*, build_url, year, **_):
    """
    This helper function uses the "build_url" input from flowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :param args: dictionary, arguments specified when running flowbyactivity.py
        flowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    urls = []

    url = build_url
    url = url.replace('__year__', str(year))
    urls.append(url)

    return urls


def bls_qcew_call(*, resp, **_):
    """
    Convert response for calling url to pandas dataframe,
    begin parsing df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    # initiate dataframes list
    df_list = []
    # unzip folder that contains bls data in ~4000 csv files
    with zipfile.ZipFile(io.BytesIO(resp.content), "r") as f:
        # read in file names
        for name in f.namelist():
            # Only want state info
            if "singlefile" in name:
                data = f.open(name)
                df_state = pd.read_csv(data, header=0, dtype=str)
                df_list.append(df_state)
                # concat data into single dataframe
                df = pd.concat(df_list, sort=False)
                df = df[['area_fips', 'own_code', 'industry_code', 'year',
                         'annual_avg_estabs', 'annual_avg_emplvl',
                         'total_annual_wages']]
        return df


def bls_qcew_parse(*, df_list, year, **_):
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # Concat dataframes
    df = pd.concat(df_list, sort=False)
    # drop rows don't need
    df = df[~df['area_fips'].str.contains(
        'C|USCMS|USMSA|USNMS')].reset_index(drop=True)
    df.loc[df['area_fips'] == 'US000', 'area_fips'] = US_FIPS
    # set datatypes
    float_cols = [col for col in df.columns if col not in
                  ['area_fips', 'own_code', 'industry_code', 'year']]
    for col in float_cols:
        df[col] = df[col].astype('float')
    # Keep owner_code = 1, 2, 3, 5
    df = df[df.own_code.isin(['1', '2', '3', '5'])]
    # replace ownership code with text defined by bls
    # https://www.bls.gov/cew/classifications/ownerships/ownership-titles.htm
    replace_dict = {'1': 'Federal Government',
                    '2': 'State Government',
                    '3': 'Local Government',
                    '5': 'Private'}
    for key in replace_dict.keys():
        df['own_code'] = df['own_code'].replace(key, replace_dict[key])
    # Rename fields
    df = df.rename(columns={'area_fips': 'Location',
                            'industry_code': 'ActivityProducedBy',
                            'year': 'Year',
                            'annual_avg_emplvl': 'Number of employees',
                            'annual_avg_estabs': 'Number of establishments',
                            'total_annual_wages': 'Annual payroll'})
    # Reformat FIPs to 5-digit
    df['Location'] = df['Location'].apply('{:0>5}'.format)
    # use "melt" fxn to convert colummns into rows
    df2 = df.melt(id_vars=["Location", "ActivityProducedBy", "Year",
                          'own_code'],
                  var_name="FlowName",
                  value_name="FlowAmount")
    # specify unit based on flowname
    df2['Unit'] = np.where(df2["FlowName"] == 'Annual payroll', "USD", "p")
    # specify class
    df2.loc[df2['FlowName'] == 'Number of employees', 'Class'] = 'Employment'
    df2.loc[df2['FlowName'] == 'Number of establishments', 'Class'] = 'Other'
    df2.loc[df2['FlowName'] == 'Annual payroll', 'Class'] = 'Money'
    # update flow name
    df2['FlowName'] = df2['FlowName'] + ', ' + df2['own_code']
    df2 = df2.drop(columns='own_code')
    # add location system based on year of data
    df2 = assign_fips_location_system(df2, year)
    # add hard code data
    df2['SourceName'] = 'BLS_QCEW'
    # Add tmp DQ scores
    df2['DataReliability'] = 5
    df2['DataCollection'] = 5
    df2['Compartment'] = None
    df2['FlowType'] = "ELEMENTARY_FLOW"

    return df2


def bls_clean_allocation_fba_w_sec(df_w_sec, **kwargs):
    """
    clean up bls df with sectors by estimating suppresed data
    :param df_w_sec: df, FBA format BLS QCEW data
    :param kwargs: additional arguments can include 'attr', a
    dictionary of FBA method yaml parameters
    :return: df, BLS QCEW FBA with estimated suppressed data
    """
    groupcols = list(df_w_sec.select_dtypes(include=['object', 'int']).columns)
    # estimate supressed data
    df = equally_allocate_suppressed_parent_to_child_naics(
        df_w_sec, kwargs['method'], 'SectorProducedBy', groupcols)

    # for purposes of allocation, we do not need to differentiate between
    # federal government, state government, local government, or private
    # sectors. So after estimating the suppressed data (above), modify the
    # flow names and aggregate data
    col_list = [e for e in df_w_sec.columns if e in ['FlowName', 'Flowable']]
    for c in col_list:
        df[c] = df[c].str.split(',').str[0]
    df2 = aggregator(df, groupcols)

    return df2


def clean_qcew(fba: FlowByActivity, **kwargs):
    #todo: check function method for state
    if fba.config.get('geoscale') == 'national':
        fba = fba.query('Location == "00000"')

    totals = (
        fba
        .query('ActivityProducedBy.str.len() == 3')
        [['Location', 'ActivityProducedBy', 'FlowAmount']]
        .assign(ActivityProducedBy=lambda x: (x.ActivityProducedBy
                                              .str.slice(stop=2)))
        .groupby(['Location', 'ActivityProducedBy']).agg('sum')
        .reset_index()
        .rename(columns={'FlowAmount': 'new_total'})
    )

    merged = fba.merge(totals, how='left')

    fixed = (
        merged
        .assign(FlowAmount=merged.FlowAmount.mask(
            (merged.ActivityProducedBy.str.len() == 2)
            & (merged.FlowAmount == 0),
            merged.new_total
        ))
        .drop(columns='new_total')
        .reset_index(drop=True)
    )

    target_naics = set(industry_spec_key(fba.config['industry_spec'])
                       .target_naics)
    filtered = (
        fixed
        .assign(ActivityProducedBy=fixed.ActivityProducedBy.mask(
            (fixed.ActivityProducedBy + '0').isin(target_naics),
            fixed.ActivityProducedBy + '0'
        ))
        .query('ActivityProducedBy in @target_naics')
    )

    return filtered


def clean_qcew_for_fbs(fba: FlowByActivity, **kwargs):
    """
    clean up bls df with sectors by estimating suppresed data
    :param df_w_sec: df, FBA format BLS QCEW data
    :param kwargs: additional arguments can include 'attr', a
    dictionary of FBA method yaml parameters
    :return: df, BLS QCEW FBA with estimated suppressed data
    """
    fba['Flowable'] = 'Jobs'
    return fba


def estimate_suppressed_qcew(fba: FlowByActivity) -> FlowByActivity:
    if fba.config.get('geoscale') == 'national':
        fba = fba.query('Location == "00000"')
    else:
        log.critical('At a subnational scale, this will take a long time.')

    indexed = (
        fba
        .assign(n2=fba.ActivityProducedBy.str.slice(stop=2),
                n3=fba.ActivityProducedBy.str.slice(stop=3),
                n4=fba.ActivityProducedBy.str.slice(stop=4),
                n5=fba.ActivityProducedBy.str.slice(stop=5),
                n6=fba.ActivityProducedBy.str.slice(stop=6),
                location=fba.Location,
                category=fba.FlowName)
        .replace({'FlowAmount': {0: np.nan},
                  'ActivityProducedBy': {'31-33': '3X',
                                         '44-45': '4X',
                                         '48-49': '4Y'},
                  'n2': {'31': '3X', '32': '3X', '33': '3X',
                         '44': '4X', '45': '4X',
                         '48': '4Y', '49': '4Y'}})
        .set_index(['n2', 'n3', 'n4', 'n5', 'n6', 'location', 'category'],
                   verify_integrity=True)
    )

    def fill_suppressed(
        flows: pd.Series,
        level: int,
        full_naics: pd.Series
    ) -> pd.Series:
        parent = flows[full_naics.str.len() == level]
        children = flows[full_naics.str.len() == level + 1]
        null_children = children[children.isna()]

        if null_children.empty or parent.empty:
            return flows
        else:
            value = max((parent[0] - children.sum()) / null_children.size, 0)
            return flows.fillna(pd.Series(value, index=null_children.index))

    unsuppressed = (
        indexed
        .assign(
            FlowAmount=lambda x: (
                x.groupby(level=['n2',
                                 'location', 'category'])['FlowAmount']
                .transform(fill_suppressed, 2, x.ActivityProducedBy)))
        .assign(
            FlowAmount=lambda x: (
                x.groupby(level=['n2', 'n3',
                                 'location', 'category'])['FlowAmount']
                .transform(fill_suppressed, 3, x.ActivityProducedBy)))
        .assign(
            FlowAmount=lambda x: (
                x.groupby(level=['n2', 'n3', 'n4',
                                 'location', 'category'])['FlowAmount']
                .transform(fill_suppressed, 4, x.ActivityProducedBy)))
        .assign(
            FlowAmount=lambda x: (
                x.groupby(level=['n2', 'n3', 'n4', 'n5',
                                 'location', 'category'])['FlowAmount']
                .transform(fill_suppressed, 5, x.ActivityProducedBy)))
        .fillna({'FlowAmount': 0})
        .reset_index(drop=True)
    )

    aggregated = (
        unsuppressed
        .assign(FlowName='Number of employees')
        .replace({'ActivityProducedBy': {'3X': '31-33',
                                         '4X': '44-45',
                                         '4Y': '48-49'}})
        .aggregate_flowby()
    )

    return aggregated
