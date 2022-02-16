# write_Crosswalk_BLM_PLS.py (scripts)
# !/usr/bin/env python3
# coding=utf-8

"""
Create a crosswalk linking the downloaded BLM_PLS to NAICS_Code_2012.
Created by selecting unique Activity Names and
manually assigning to NAICS

"""
import pandas as pd
from flowsa.settings import crosswalkpath
from scripts.common_scripts import unique_activity_names, order_crosswalk


def assign_naics(df):
    """
    Function to assign NAICS codes to each dataframe activity

    Sector assignments are based off Table 4
    https://www.epa.gov/sites/default/files/2020-11/documents/2018_wasted_food_report-11-9-20_final_.pdf

    :param df: df, a FlowByActivity subset that contains unique activity names
    :return: df with assigned Sector columns
    """

    # Activities producing the food waste
    # Activities with NAICS defined in Table 4 of the report
    df.loc[df['Activity'] == 'Correctional Facilities', 'Sector'] = \
        pd.Series([['92214', '5612']]*df.shape[0])
    df.loc[df['Activity'] == 'Food Banks', 'Sector'] = '62421'
    df.loc[df['Activity'] == 'Hospitals', 'Sector'] = '622'
    df.loc[df['Activity'] == 'Hotels', 'Sector'] = \
        pd.Series([['7211', '71321']]*df.shape[0])
    df.loc[df['Activity'] == 'Manufacturing/Processing', 'Sector'] = \
        pd.Series([['3112', '3113', '3114', '3115', '3116', '3117', '3118',
                    '3119', '312111', '31212', '31213', '31214']]*df.shape[0])
    df.loc[df['Activity'] == 'Nursing Homes', 'Sector'] = '623'
    df.loc[df['Activity'] == 'Restaurants/Food Services', 'Sector'] = \
        pd.Series([['7225', '72232', '72233']]*df.shape[0])
    df.loc[df['Activity'] == 'Retail', 'Sector'] = \
        pd.Series([['4451', '4452', '45291']]*df.shape[0])
    df.loc[df['Activity'] == 'Wholesale', 'Sector'] = '4244'

    # Activities where NAICS are not defined in T4 of report
    # method for colleges based on 4 year colleges
    # todo: assign to 4 year colleges only? (method results based on) or all
    #  related higher ed? junior colleges? trade schools?
    df.loc[df['Activity'] == 'Colleges & Universities', 'Sector'] = '6113'
    df.loc[df['Activity'] == 'K-12 Schools', 'Sector'] = '6111'
    df.loc[df['Activity'] == 'Military Installations', 'Sector'] = '92811'
    # use office building sector assignments identified by EIA CBECS
    # TODO: List currently duplicates some of the other assignments - modify
    #  list when other sectors finalized
    df.loc[df['Activity'] == 'Office Buildings', 'Sector'] = \
        pd.Series([['423', '424', '441', '454', '481', '482', '483', '484',
                    '485', '486', '487', '488', '492', '493', '511', '512',
                    '515', '516', '517', '518', '519', '521', '522', '523',
                    '524', '525', '531', '532', '533', '541', '551', '561',
                    '562', '611', '621', '624', '711', '712', '713', '811',
                    '813', '921', '922', '923', '924', '925', '926', '927',
                    '928']]*df.shape[0])
    # Household code
    df.loc[df['Activity'] == 'Residential', 'Sector'] = 'F010'
    # todo: assign to "promoters of performing arts, sports and similar
    #  events with facilities" (71131)?
    df.loc[df['Activity'] == 'Sports Venues', 'Sector'] = ''

    # Activities consuming the food waste
    # todo: assign animal feed to animal production sector (112)?
    df.loc[df['Activity'] == 'Animal Feed', 'Sector'] = ''
    # todo: assign to biomass electric gen (221117)?
    df.loc[df['Activity'] == 'Bio-based Materials/Biochemical Processing',
           'Sector'] = ''
    # todo: assign...
    df.loc[df['Activity'] == 'Codigestion/Anaerobic Digestion', 'Sector'] = ''
    # todo: assign to "Fertilizer (Mixing Only) Manufacturing" for "compost
    #  manufacturing" (325314) or "Other Nonhazardous Waste Treatment and
    #  Disposal" for the "compost dumps" component (562219)
    df.loc[df['Activity'] == 'Composting/Aerobic Processes', 'Sector'] = ''
    df.loc[df['Activity'] == 'Controlled Combustion', 'Sector'] = '562213'
    # todo: assign...
    df.loc[df['Activity'] == 'Food Donation', 'Sector'] = ''
    # todo: assign land application to farming sector (111)?
    df.loc[df['Activity'] == 'Land Application', 'Sector'] = ''
    df.loc[df['Activity'] == 'Landfill', 'Sector'] = '562212'
    df.loc[df['Activity'] == 'Sewer/Wastewater Treatment', 'Sector'] = '22132'

    # break each sector into seperate line
    df = df.explode('Sector')

    return df


if __name__ == '__main__':
    # select years to pull unique activity names
    years = ['2018']
    # assign datasource
    datasource = 'EPA_WFR'
    # df of unique ers activity names
    df_list = []
    for y in years:
        dfy = unique_activity_names(datasource, y)
        df_list.append(dfy)
    df = pd.concat(df_list, ignore_index=True).drop_duplicates()
    # add manual naics 2012 assignments
    df = assign_naics(df)
    # assign sector source name
    df['SectorSourceName'] = 'NAICS_2012_Code'
    # drop any rows where naics12 is 'nan'
    # (because level of detail not needed or to prevent double counting)
    df.dropna(subset=["Sector"], inplace=True)
    # assign sector type
    df['SectorType'] = "I"
    # sort df
    df = order_crosswalk(df)
    # save as csv
    df.to_csv(f"{crosswalkpath}NAICS_Crosswalk_{datasource}.csv",
              index=False)
