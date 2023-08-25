import esupy.processed_data_mgmt
import pandas as pd
from pandas import ExcelWriter
from flowsa import settings, metadata, common, exceptions, log, geo, naics
from flowsa.common import get_catalog_info
from flowsa.flowby import _FlowBy, flowby_config, get_flowby_from_config
from flowsa.flowsa_log import reset_log_file


class FlowBySector(_FlowBy):
    _metadata = [*_FlowBy()._metadata]

    def __init__(
        self,
        data: pd.DataFrame or '_FlowBy' = None,
        *args,
        collapsed: bool = False,
        w_activity: bool = False,
        **kwargs
    ) -> None:
        if isinstance(data, pd.DataFrame):
            collapsed = collapsed or any(
                [c in data.columns for c in flowby_config['_collapsed_fields']]
            )
            w_activity = w_activity or any(
                [c in data.columns for c in flowby_config['_activity_fields']]
            )

            if collapsed:
                fields = flowby_config['fbs_collapsed_fields']
            elif w_activity:
                fields = flowby_config['fbs_w_activity_fields']
            else:
                fields = flowby_config['fbs_fields']

            column_order = flowby_config['fbs_column_order']
        else:
            fields = None
            column_order = None

        super().__init__(data,
                         fields=fields,
                         column_order=column_order,
                         *args, **kwargs)

    @property
    def _constructor(self) -> 'FlowBySector':
        return FlowBySector

    @property
    def _constructor_sliced(self) -> '_FBSSeries':
        from flowsa.flowbyseries import _FBSSeries
        return _FBSSeries

    @classmethod
    def getFlowBySector(
        cls,
        method: str,
        config: dict = None,
        external_config_path: str = None,
        download_sources_ok: bool = settings.DEFAULT_DOWNLOAD_IF_MISSING,
        download_fbs_ok: bool = settings.DEFAULT_DOWNLOAD_IF_MISSING,
        **kwargs
    ) -> 'FlowBySector':
        '''
        Loads stored FlowBySector output. If it is not
        available, tries to download it from EPA's remote server (if
        download_ok is True), or generate it.
        :param method: string, name of the FBS attribution method file to use
        :param external_config_path: str, path to the FBS method file if
            loading a file from outside the flowsa repository
        :param download_fba_ok: bool, if True will attempt to load FBAs
            used in generating the FBS from EPA's remote server rather than
            generating (if not found locally)
        :param download_FBS_if_missing: bool, if True will attempt to load the
            FBS from EPA's remote server rather than generating it
            (if not found locally)
        :kwargs: keyword arguments to pass to _getFlowBy(). Possible kwargs
            include full_name and config.
        :return: FlowBySector dataframe
        '''
        file_metadata = metadata.set_fb_meta(method, 'FlowBySector')

        if config is None:
            try:
                config = common.load_yaml_dict(method, 'FBS',
                                               external_config_path)
            except exceptions.FlowsaMethodNotFoundError:
                config = {}

        flowby_generator = (
            lambda x=method, y=external_config_path, z=download_sources_ok:
                cls.generateFlowBySector(x, y, z)
            )
        return super()._getFlowBy(
            file_metadata=file_metadata,
            download_ok=download_fbs_ok,
            flowby_generator=flowby_generator,
            output_path=settings.fbsoutputpath,
            full_name=method,
            config=config,
            **kwargs
        )

    @classmethod
    def generateFlowBySector(
        cls,
        method: str,
        external_config_path: str = None,
        download_sources_ok: bool = settings.DEFAULT_DOWNLOAD_IF_MISSING,
    ) -> 'FlowBySector':
        '''
        Generates a FlowBySector dataset.
        :param method: str, name of FlowBySector method .yaml file to use.
        :param external_config_path: str, optional. If given, tells flowsa
            where to look for the method yaml specified above.
        :param download_fba_ok: bool, optional. Whether to attempt to download
            source data FlowByActivity files from EPA server rather than
            generating them.
        '''
        log.info('Beginning FlowBySector generation for %s', method)
        method_config = common.load_yaml_dict(method, 'FBS',
                                              external_config_path)

        # Cache one or more sources by attaching to method_config
        to_cache = method_config.pop('sources_to_cache', {})
        if 'cache' in method_config:
            log.warning('Config key "cache" for %s about to be overwritten',
                        method)

        method_config['cache'] = {}
        for source_name, config in to_cache.items():
            method_config['cache'][source_name] = (
                get_flowby_from_config(
                    name=source_name,
                    config={
                        **method_config,
                        'method_config_keys': method_config.keys(),
                        **get_catalog_info(source_name),
                        **config
                    },
                    external_config_path=external_config_path,
                    download_sources_ok=download_sources_ok
                ).prepare_fbs(external_config_path=external_config_path, download_sources_ok=download_sources_ok)
            )
            # ^^^ This is done with a for loop instead of a dict comprehension
            #     so that later entries in method_config['sources_to_cache']
            #     can make use of the cached copy of an earlier entry.

        # Generate FBS from method_config
        sources = method_config.pop('source_names')

        fbs = pd.concat([
            get_flowby_from_config(
                name=source_name,
                config={
                    **method_config,
                    'method_config_keys': method_config.keys(),
                    **get_catalog_info(source_name),
                    **config
                },
                external_config_path=external_config_path,
                download_sources_ok=download_sources_ok
            ).prepare_fbs(external_config_path=external_config_path,
                          download_sources_ok=download_sources_ok)
            for source_name, config in sources.items()
        ])

        fbs.full_name = method
        fbs.config = method_config

        # drop year from LocationSystem for FBS use with USEEIO
        fbs['LocationSystem'] = fbs['LocationSystem'].str.split('_').str[0]
        # aggregate to target geoscale
        fbs = (
            fbs
            .convert_fips_to_geoscale(
                geo.scale.from_string(fbs.config.get('geoscale')))
            .aggregate_flowby()
        )
        # aggregate to target sector
        fbs = fbs.sector_aggregation()

        # set all data quality fields to none until implemented fully
        log.info('Reset all data quality fields to None')
        dq_cols = ['Spread', 'Min', 'Max',
                   'DataReliability', 'TemporalCorrelation',
                   'GeographicalCorrelation', 'TechnologicalCorrelation',
                   'DataCollection']
        fbs = fbs.assign(**dict.fromkeys(dq_cols, None))

        # Save fbs and metadata
        log.info(f'FBS generation complete, saving {method} to file')
        meta = metadata.set_fb_meta(method, 'FlowBySector')
        esupy.processed_data_mgmt.write_df_to_file(fbs, settings.paths, meta)
        reset_log_file(method, meta)
        metadata.write_metadata(source_name=method,
                                config=common.load_yaml_dict(
                                    method, 'FBS', external_config_path),
                                fb_meta=meta,
                                category='FlowBySector')

        return fbs

    def sector_aggregation(self):
        """
        In the event activity sets in an FBS are at a less aggregated target
        sector level than the overall target level, aggregate the sectors to
        the FBS target scale
        :return:
        """
        naics_key = naics.industry_spec_key(self.config['industry_spec'])

        fbs = self
        for direction in ['ProducedBy', 'ConsumedBy']:
            fbs = (
                fbs
                .rename(columns={f'Sector{direction}': 'source_naics'})
                .merge(naics_key,
                       how='left')
                .rename(columns={'target_naics': f'Sector{direction}'})
                .drop(columns='source_naics')
                .aggregate_flowby()
            )

        return fbs

    def prepare_fbs(
        self: 'FlowBySector',
        external_config_path: str = None,
        download_sources_ok: bool = True
    ) -> 'FlowBySector':
        if 'activity_sets' in self.config:
            try:
                return (
                    pd.concat([
                        fbs.prepare_fbs()
                        for fbs in (
                            self
                            .select_by_fields()
                            .activity_sets()
                        )
                    ])
                    .reset_index(drop=True)
                )
            except ValueError:
                return FlowBySector(pd.DataFrame())
        return (
            self
            .function_socket('clean_fbs')
            .select_by_fields()
            .sector_aggregation()  # convert to proper industry spec.
            .convert_fips_to_geoscale()
            .attribute_flows_to_sectors(external_config_path=external_config_path,
                                        download_sources_ok=download_sources_ok)
            .aggregate_flowby()  # necessary after consolidating geoscale
        )


    def display_tables(
        self: 'FlowBySector',
        display_tables: dict = None
    ) -> pd.DataFrame:
        display_tables = display_tables or self.config.get('display_tables')
        if display_tables is None:
            log.error('Cannot generate display tables, since no configuration '
                      'is specified for them')
            return None

        def convert_industry_spec(
            fb_at_source_naics: 'FlowBySector',
            industry_spec: dict = None
        ) -> 'FlowBySector':
            '''
            This is here because it's only for display purposes. It can be
            replaced once there's a proper method for converting an FBS to
            a new industry_spec
            '''
            if industry_spec is None:
                return fb_at_source_naics
            fb_at_target_naics = (
                fb_at_source_naics
                .merge(naics.industry_spec_key(industry_spec),
                       how='left',
                       left_on='SectorProducedBy', right_on='source_naics')
                .assign(
                    SectorProducedBy=lambda x:
                        x.SectorProducedBy.mask(x.SectorProducedBy.str.len()
                                                >= x.target_naics.str.len(),
                                                x.target_naics)
                )
                .drop(columns=['target_naics', 'source_naics'])
                .aggregate_flowby()
            )
            return fb_at_target_naics

        table_dict = {
            table_name: (
                self
                .select_by_fields(table_config.get('selection_fields'))
                .pipe(convert_industry_spec, table_config.get('industry_spec'))
                [['Flowable', 'Unit', 'SectorProducedBy', 'FlowAmount']]
                .rename(columns={'Flowable': 'Pollutant',
                                 'SectorProducedBy': 'Industry',
                                 'FlowAmount': 'Amount'})
                .replace(table_config.get('replace_dict', {}))
                .assign(Pollutant=lambda x: x.Pollutant + ' (' + x.Unit + ')')
                .drop(columns='Unit')
                .groupby(['Pollutant', 'Industry']).agg('sum')
                .reset_index()
                .pivot(index='Pollutant', columns='Industry', values='Amount')
            )
            for table_name, table_config in display_tables.items()
        }

        tables_path = (settings.tableoutputpath / f'{self.full_name}'
                       f'_Display_Tables.xlsx')
        try:
            with ExcelWriter(tables_path) as writer:
                for name, table in table_dict.items():
                    table.to_excel(writer, name)
        except PermissionError:
            log.warning(f'Permission to write display tables for '
                        f'{self.full_name} to {tables_path} denied.')

        return table_dict
