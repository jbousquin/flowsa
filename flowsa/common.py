# common.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""Common variables and functions used across flowsa"""

import os
from os import path
import yaml
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import flowsa.flowsa_yaml as flowsa_yaml
import flowsa.exceptions
from flowsa.flowsa_log import log
from flowsa.settings import datapath, MODULEPATH, \
    sourceconfigpath, flowbysectormethodpath, methodpath


# Sets default Sector Source Name
SECTOR_SOURCE_NAME = 'NAICS_2012_Code'
flow_types = ['ELEMENTARY_FLOW', 'TECHNOSPHERE_FLOW', 'WASTE_FLOW']

sector_level_key = {"NAICS_2": 2,
                    "NAICS_3": 3,
                    "NAICS_4": 4,
                    "NAICS_5": 5,
                    "NAICS_6": 6}

# withdrawn keyword changed to "none" over "W"
# because unable to run calculation functions with text string
WITHDRAWN_KEYWORD = np.nan


def load_env_file_key(env_file, key):
    """
    Loads an API Key from "API_Keys.env" file using the
    'api_name' defined in the FBA source config file. The '.env' file contains
    the users personal API keys. The user must register with this
    API and get the key and manually add to "API_Keys.env"

    See wiki for how to get an api:
    https://github.com/USEPA/flowsa/wiki/Using-FLOWSA#api-keys

    :param env_file: str, name of env to load, either 'API_Key'
    or 'external_path'
    :param key: str, name of source/key defined in env file, like 'BEA' or
    'Census'
    :return: str, value of the key stored in the env
    """
    if env_file == 'API_Key':
        load_dotenv(f'{MODULEPATH}API_Keys.env', verbose=True)
        value = os.getenv(key)
        if value is None:
            raise flowsa.exceptions.APIError(api_source=key)
    else:
        load_dotenv(f'{MODULEPATH}external_paths.env', verbose=True)
        value = os.getenv(key)
        if value is None:
            raise flowsa.exceptions.EnvError(key=key)
    return value


def load_crosswalk(crosswalk_name):
    """
    Load NAICS crosswalk between the years 2007, 2012, 2017
    :return: df, NAICS crosswalk over the years
    """

    cw_dict = {'sector_timeseries': 'NAICS_Crosswalk_TimeSeries',
               'sector_length': 'NAICS_2012_Crosswalk',
               'sector_name': 'Sector_2012_Names',
               'household': 'Household_SectorCodes',
               'government': 'Government_SectorCodes',
               'BEA': 'NAICS_to_BEA_Crosswalk'
               }

    fn = cw_dict.get(crosswalk_name)

    cw = pd.read_csv(f'{datapath}{fn}.csv', dtype="str")
    return cw


def load_sector_length_cw_melt():
    cw_load = load_crosswalk('sector_length')
    cw_melt = cw_load.melt(var_name="SectorLength", value_name='Sector'
                           ).drop_duplicates().reset_index(drop=True)
    cw_melt = cw_melt.dropna().reset_index(drop=True)
    cw_melt['SectorLength'] = cw_melt['SectorLength'].str.replace(
        'NAICS_', "")
    cw_melt['SectorLength'] = pd.to_numeric(cw_melt['SectorLength'])

    cw_melt = cw_melt[['Sector', 'SectorLength']]

    return cw_melt


def return_bea_codes_used_as_naics():
    """

    :return: list of BEA codes used as NAICS
    """
    cw_list = []
    for cw in ['household', 'government']:
        df = load_crosswalk(cw)
        cw_list.append(df)
    # concat data into single dataframe
    cw = pd.concat(cw_list, sort=False)
    code_list = cw['Code'].drop_duplicates().values.tolist()
    return code_list


def load_yaml_dict(filename, flowbytype=None, filepath=None):
    """
    Load the information in a yaml file, from source_catalog, or FBA,
    or FBS files
    :return: dictionary containing all information in yaml
    """
    if filename in ['source_catalog']:
        folder = datapath
    else:
        # first check if a filepath for the yaml is specified, as is the
        # case with FBS method files located outside FLOWSA
        # if filepath is not None:
        if path.exists(path.join(str(filepath), f'{filename}.yaml')):
            log.info(f'Loading {filename} from {filepath}')
            folder = filepath
        else:
            if filepath is not None:
                log.warning(f'{filename} not found in {filepath}. '
                            f'Checking default folders')
            if flowbytype == 'FBA':
                folder = sourceconfigpath
            elif flowbytype == 'FBS':
                folder = flowbysectormethodpath
            else:
                raise KeyError('Must specify either \'FBA\' or \'FBS\'')
    yaml_path = f'{folder}/{filename}.yaml'

    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            config = flowsa_yaml.load(f, filepath)
    except FileNotFoundError:
        raise flowsa.exceptions.FlowsaMethodNotFoundError(
            method_type=flowbytype, method=filename)
    return config


def load_values_from_literature_citations_config():
    """
    Load the config file that contains information on where the
    values from the literature come from
    :return: dictionary of the values from the literature information
    """
    sfile = (f'{datapath}bibliographyinfo/'
             f'values_from_literature_source_citations.yaml')
    with open(sfile, 'r') as f:
        config = yaml.safe_load(f)
    return config


def clean_str_and_capitalize(s):
    """
    Trim whitespace, modify string so first letter capitalized.
    :param s: str
    :return: str, formatted
    """
    if s.__class__ == str:
        s = s.strip()
        s = s.lower()
        s = s.capitalize()
    return s


def capitalize_first_letter(string):
    """
    Capitalize first letter of words
    :param string: str
    :return: str, modified
    """
    return_string = ""
    split_array = string.split(" ")
    for s in split_array:
        return_string = return_string + " " + s.capitalize()
    return return_string.strip()


def get_flowsa_base_name(filedirectory, filename, extension):
    """
    If filename does not match filename within flowsa due to added extensions
    onto the filename, cycle through
    name, dropping strings after each underscore until the name is found
    :param filedirectory: string, path to directory
    :param filename: string, name of original file searching for
    :param extension: string, type of file, such as "yaml" or "py"
    :return: string, corrected file path name
    """
    # If a file does not exist, modify file name, dropping portion after last
    # underscore. Repeat this process until the file name exists or no
    # underscores are left.
    while '_' in filename:
        if os.path.exists(f"{filedirectory}{filename}.{extension}"):
            break
        filename, _ = filename.rsplit('_', 1)

    return filename


def return_true_source_catalog_name(sourcename):
    """
    Drop any extensions on source name until find the name in source catalog
    """
    while (load_yaml_dict('source_catalog').get(sourcename) is None) & (
            '_' in sourcename):
        sourcename = sourcename.rsplit("_", 1)[0]
    return sourcename


def check_activities_sector_like(df_load, sourcename=None):
    """
    Check if the activities in a df are sector-like,
    if cannot find the sourcename in the source catalog, drop extensions on the
    source name
    :param df_load: df, df to determine if activities are sector-like
    :param source: str, optionial, can identify sourcename to use
    """
    # identify sourcename
    if sourcename is not None:
        s = sourcename
    else:
        if 'SourceName' in df_load.columns:
            s = pd.unique(df_load['SourceName'])[0]
        elif 'MetaSources' in df_load.columns:
            s = pd.unique(df_load['MetaSources'])[0]

    sourcename = return_true_source_catalog_name(s)

    try:
        sectorLike = load_yaml_dict('source_catalog')[sourcename][
            'sector-like_activities']
    except KeyError:
        log.info(f'%s not found in {datapath}source_catalog.yaml, assuming '
                 f'activities are not sector-like', sourcename)
        sectorLike = False

    return sectorLike


def str2bool(v):
    """
    Convert string to boolean
    :param v: string
    :return: boolean
    """
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    else:
        return False


def check_method_status():
    """Read the current method status"""
    yaml_path = methodpath + 'method_status.yaml'
    with open(yaml_path, 'r') as f:
        method_status = yaml.safe_load(f)
    return method_status
