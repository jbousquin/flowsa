# USGS_MYB_Tungsten.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

import io
from flowsa.flowbyfunctions import assign_fips_location_system
from flowsa.data_source_scripts.USGS_MYB_Common import *
from flowsa.common import WITHDRAWN_KEYWORD


"""
Projects
/
FLOWSA
/

FLOWSA-314

Import USGS Mineral Yearbook data

Description

Table T1

SourceName: USGS_MYB_Tungsten
https://www.usgs.gov/centers/nmic/tungsten-statistics-and-information

Minerals Yearbook, xls file, tab T1

Data for: Tungsten; concentrate

Years = 2014+
"""
SPAN_YEARS = "2013-2017"


def usgs_tungsten_url_helper(build_url, config, args):
    """
    This helper function uses the "build_url" input from flowbyactivity.py, which
    is a base url for data imports that requires parts of the url text string
    to be replaced with info specific to the data year.
    This function does not parse the data, only modifies the urls from which data is obtained.
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :param args: dictionary, arguments specified when running flowbyactivity.py
        flowbyactivity.py ('year' and 'source')
    :return: list, urls to call, concat, parse, format into Flow-By-Activity format
    """
    url = build_url
    return [url]


def usgs_tungsten_call(url, r, args):
    """
    Convert response for calling url to pandas dataframe, begin parsing df into FBA format
    :param url: string, url
    :param r: df, response from url call
    :param args: dictionary, arguments specified when running
        flowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    df_raw_data = pd.io.excel.read_excel(io.BytesIO(r.content), sheet_name='T1')
    df_data = pd.DataFrame(df_raw_data.loc[7:10]).reindex()
    df_data = df_data.reset_index()
    del df_data["index"]


    if len(df_data. columns) == 11:
        df_data.columns = ["Production", "space_1", "year_1", "space_2", "year_2", "space_3",
                             "year_3", "space_4", "year_4", "space_5", "year_5"]

    col_to_use = ["Production"]
    col_to_use.append(usgs_myb_year(SPAN_YEARS, args["year"]))
    for col in df_data.columns:
        if col not in col_to_use:
            del df_data[col]

    return df_data


def usgs_tungsten_parse(dataframe_list, args):
    """
    Combine, parse, and format the provided dataframes
    :param dataframe_list: list of dataframes to concat and format
    :param args: dictionary, used to run flowbyactivity.py ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity specifications
    """
    data = {}
    row_to_use = ["Production", "Exports", "Imports for consumption"]
    prod = ""
    name = usgs_myb_name(args["source"])
    des = name
    dataframe = pd.DataFrame()
    col_name = usgs_myb_year(SPAN_YEARS, args["year"])
    for df in dataframe_list:
        for index, row in df.iterrows():
            if df.iloc[index]["Production"].strip() == "Imports for consumption":
                product = "imports"
            elif df.iloc[index]["Production"].strip() == "Production":
                product = "production"
            elif df.iloc[index]["Production"].strip() == "Exports":
                product = "exports"



            if df.iloc[index]["Production"].strip() in row_to_use:
                data = usgs_myb_static_varaibles()
                data["SourceName"] = args["source"]
                data["Year"] = str(args["year"])
                data["Unit"] = "Metric Tons"
                data['FlowName'] = name + " " + product
                data["Description"] = name
                data["ActivityProducedBy"] = name
                col_name = usgs_myb_year(SPAN_YEARS, args["year"])
                if str(df.iloc[index][col_name]) == "--":
                    data["FlowAmount"] = str(0)
                elif str(df.iloc[index][col_name]) == "nan":
                    data["FlowAmount"] = WITHDRAWN_KEYWORD
                else:
                    data["FlowAmount"] = str(df.iloc[index][col_name])
                dataframe = dataframe.append(data, ignore_index=True)
                dataframe = assign_fips_location_system(dataframe, str(args["year"]))
    return dataframe

