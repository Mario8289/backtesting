import os
import yaml
import datetime as dt
import json
import logging


class GetArgs:

    @staticmethod
    def validate_inputs(w):
        if w['start_date'].value == None:
            raise ValueError("you must add a start date to the backtest simulator")
        if w['end_date'].value == None:
            raise ValueError("you must add a end date to the backtest simulator")

    @staticmethod
    def get_default_args():
        return {
            'auth_path': '/home/jovyan/work/lmax_analytics_notebooks/notebooks/auth/backtesting_credentials.yaml',
            'bucket': 'risk-temp',
            'scenario_path': None,
            'scenario': None,
            'simulations_filter': None,
            'pipeline_path': None,
            'simulations_path': None,
            'output_path': None,
            'return_results': True
        }

    @staticmethod
    def get_default_pipeline_config():
        return {
            'load_starting_positions': True,
            'process_client_portfolio': False,
            'process_lmax_portfolio': True,
            'simulator': 'simulator_pool',
            'store_client_trade_snapshot': False,
            'store_lmax_trade_snapshot': True,
            'version': 1
        }

    @staticmethod
    def get_default_simulations_config():
        return {
            'constructor': 'product',
            'instruments': None,
            'risk_parameters': {'risk_type': 'no_risk'},
            'split_by_instrument': True
        }

    @staticmethod
    def get_default_output_config():
        return {
            'bucket': 'risk-temp',
            'directory': 'backtesting/simulation_outputs',
            'event_features': ['symbol', 'order_book_id', 'account_id'],
            'file': None,
            'by': 'trading_session',
            'filesystem': 's3',
            'freq': 'D',
            'store_index': False
        }

    @staticmethod
    def dict_to_yaml(data: dict, directory: str, filename: str):
        with open(os.path.join(directory, filename), 'w') as file:
            yaml.dump(data, file)

    @staticmethod
    def load_json(string, error_tag):
        try:
            value = json.loads(string)
        except ValueError as e:
            raise ValueError(f"check your {error_tag}: {e}")
        return value

    def create_pipeline_config(self, w):
        config = self.get_default_pipeline_config()
        config['uid'] = w['uid'].value
        config['shard'] = w['shard'].value
        config['lmax_account'] = w['lmax_account'].value
        exclusion_window = [list(map(int, x.split(':'))) for x in
                            w['exclusion_window'].value.replace(' ', '').split('-')] if w[
                                                                                            'exclusion_window'].value != '' else None
        if w['event_stream'].value == 'event_sample_stream':
            config['event_stream_parameters'] = {
                'event_stream_type': 'event_stream_sample',
                'excl_period': exclusion_window,
                'sample_rate': 0.3
            }
        elif w['event_stream'].value == 'event_stream_snapshot':
            config['event_stream_parameters'] = {
                'event_stream_type': 'event_stream_snapshot',
                'excl_period': exclusion_window,
                'sample_rate': 's'
            }
        config['matching_engine_parameters'] = {'matching_engine_type': w['matching_engine'].value}
        config['level'] = 'mark_to_market'
        config['matching_method'] = 'side_of_book'
        config['netting_engine'] = 'fifo'
        config['store_client_trade_snapshot'] = False
        return config

    def create_output_config(self, w):
        config = self.get_default_output_config()
        config['metrics'] = list(w['metrics'].value) if w['metrics'].value != None else None
        config['resample_rule'] = w['resample_rule'].value
        config['save'] = w['save'].value
        config['mode'] = w['mode'].value
        return config

    def create_simulations_config(self, w):
        config = self.get_default_simulations_config()
        config['event_filter_string'] = w['event_filter_string'].value
        if not w['load_instruments_from_snapshot'].value:
            config['instruments'] = list(w['instruments'].value)
        config['load_position_limits_from_snapshot'] = w['load_position_limits_from_snapshot'].value
        config['load_target_accounts_from_snapshot'] = w['load_target_accounts_from_snapshot'].value
        config['filter_snapshot_for_strategy'] = w['filter_snapshot_for_strategy'].value
        config['filter_snapshot_for_traded_account_instrument_pairs'] = w[
            'filter_snapshot_for_traded_account_instrument_pairs'].value
        config['exit_parameters'] = self.load_json(w['exit_parameters'].children[0].value, error_tag='exit_parameters')
        config['strategy_parameters'] = self.load_json(w['strategy_parameters'].children[0].value,
                                                       error_tag='strategy_parameters')
        return config

    def get_args(self, w, server):
        logger = logging.getLogger("getArgs")

        self.validate_inputs(w)
        args = self.get_default_args()

        args['minio_uri'] = f'http://lddata-{server}-stormd01-db-server.ix3.lmax:9000'
        args['filesystem_type'] = w['filesystem'].value
        args['target_accounts_path'] = w['accounts'].value
        args['num_cores'] = w['cores'].value
        args['num_batches'] = w['batch'].value
        args['start_date'] = dt.datetime.strftime(w['start_date'].value, '%Y-%m-%d')
        args['end_date'] = dt.datetime.strftime(w['end_date'].value, '%Y-%m-%d')

        if w['load_scenario'].value:
            logger.info('load config from scenario')
            args[
                'scenario_path'] = '/home/jovyan/work/lmax_analytics_notebooks/notebooks/backtesting_strategies/config/scenarios'
            args['scenario'] = w['scenario'].value

            if not w['scenario_sims'].disabled:
                args['simulations_filter'] = list(w['scenario_sims'].value)

        else:
            gui_dir = '/home/jovyan/work/lmax_analytics_notebooks/notebooks/backtesting_strategies/config/gui'
            if not os.path.exists(gui_dir):
                os.makedirs(gui_dir)

            if w['load_pipeline_config_from_file'].value:
                logger.info('pipeline config loaded from from file')
                args['pipeline_path'] = w['pipeline_config'].value
            # if you are using the gui to create a pipeline config then you need to write the file to disk
            else:
                logger.info('pipeline config created via gui')
                pipeline_file = 'pipeline.yaml'
                pipeline_config = self.create_pipeline_config(w)
                if os.path.exists(os.path.join(gui_dir, pipeline_file)):
                    os.remove(os.path.join(gui_dir, pipeline_file))
                self.dict_to_yaml(pipeline_config, gui_dir, pipeline_file)
                args['pipeline_path'] = os.path.join(gui_dir, pipeline_file)

            if w['load_simulation_config_from_file'].value:
                logger.info('simulations config loaded from file')
                args['simulations_path'] = w['simulations_config'].value
                if not w['sims'].disabled:
                    args['simulations_filter'] = list(w['sims'].value)
            else:
                logger.info('simulations config created via gui')
                simulations_file = 'simulations.yaml'
                simulation = self.create_simulations_config(w)
                simulations_config = {w['simulation_reference'].value: simulation}

                if os.path.exists(os.path.join(gui_dir, simulations_file)):
                    os.remove(os.path.join(gui_dir, simulations_file))
                self.dict_to_yaml(simulations_config, gui_dir, simulations_file)
                args['simulations_path'] = os.path.join(gui_dir, simulations_file)

            if w['load_output_config_from_file'].value:
                logger.info('output config loaded from file')
                args['output_path'] = w['output_config'].value

            else:
                logger.info('output config created via gui')
                output_file = 'output.yaml'
                output_config = self.create_output_config(w)
                if os.path.exists(os.path.join(gui_dir, output_file)):
                    os.remove(os.path.join(gui_dir, output_file))
                self.dict_to_yaml(output_config, gui_dir, output_file)
                args['output_path'] = os.path.join(gui_dir, output_file)

        return args