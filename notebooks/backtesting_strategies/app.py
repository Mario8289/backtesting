from ipywidgets import widgets, interactive_output
import glob
import os, yaml
import s3fs
from ipywidgets import AppLayout

from get_args import GetArgs

EXIT_PARAMETERS_DEFAULT = \
    """{"exit_type": "aggressive",
    "stoploss_limit": [50, 100, 150],
    "takeprofit_limit": [50, 100, 150]}"""

STRATEGY_PARAMETERS_DEFAULT = \
    """{"strategy_type": "internalisation",
    "max_pos_qty": [100, 200, 300],
    "max_pos_qty_buffer": 1.25,
    "position_lifespan": "gfw",
    "position_lifespan_exit_parameters":
      {"exit_type": "chaser",
      "downtick": 2,
      "maxdowntick": 100,
      "maxuptick": 2,
      "starttick": 0,
      "uptick": 2}
    }"""


class BackTesterApp(GetArgs):
    def __init__(self, snapshot, fs: s3fs.S3FileSystem = None):
        self.ldsnapshot = snapshot
        self.fs = fs
        self.w: dict = dict()

    @staticmethod
    def get_scenario_config(value):
        if value == 'local':
            return [(os.path.basename(x), x) for x in
                    glob.glob(
                        "/home/jovyan/work/lmax_analytics_notebooks/notebooks/backtesting_strategies/config/simulations/*.yaml")]

    def load_yaml(self, file):
        if self.w['filesystem'].value == 's3':
            with self.fs.open(os.path.join('risk-temp', file), mode='rb', refresh=True) as f:
                obj = yaml.safe_load(f.read())
        elif self.w['filesystem'].value == 'local':
            with open(file, 'r') as f:
                obj = yaml.safe_load(f)
        return obj

    def get_pipeline_config(self, value):
        if value == 'local':
            return [(os.path.basename(x), x) for x in
                    glob.glob(
                        "/home/jovyan/work/lmax_analytics_notebooks/notebooks/backtesting_strategies/config/pipeline/*.yaml")]
        elif value == 's3':
            return [(os.path.basename(x), os.path.join(*x.split('/')[1:])) for x in
                    self.fs.ls('risk-temp/backtesting/pipeline_config', refresh=True)]

    def get_output_config(self, value):
        if value == 'local':
            return [(os.path.basename(x), x) for x in
                    glob.glob(
                        "/home/jovyan/work/lmax_analytics_notebooks/notebooks/backtesting_strategies/config/output/*.yaml")]
        elif value == 's3':
            return [(os.path.basename(x), os.path.join(*x.split('/')[1:])) for x in
                    self.fs.ls('risk-temp/backtesting/output_config', refresh=True)]

    def get_simulations_config(self, value):
        if value == 'local':
            return [(os.path.basename(x), x) for x in
                    glob.glob(
                        "/home/jovyan/work/lmax_analytics_notebooks/notebooks/backtesting_strategies/config/simulations/*.yaml")]
        elif value == 's3':
            return [(os.path.basename(x), os.path.join(*x.split('/')[1:])) for x in
                    self.fs.ls('risk-temp/backtesting/simulations_config', refresh=True)]

    def get_accounts(self, value):
        if value == 'local':
            account_list = [(os.path.basename(x), x) for x in
                            glob.glob(
                                "/home/jovyan/work/lmax_analytics_notebooks/notebooks/backtesting_strategies/account_list/*.csv")]
        elif value == 's3':
            account_list = [(os.path.basename(x), os.path.join(*x.split('/')[1:])) for x in
                            self.fs.ls('risk-temp/backtesting/account_list', refresh=True)]
        return [("NO ACCOUNT LIST", None)] + account_list

    def get_instruments(self, value, property_type='value'):
        if value == 'ldprof':
            options = list(sorted(list(map(lambda x: tuple(x), self.ldsnapshot[['symbol', 'instrument_id']].drop_duplicates().values.tolist()))))
            values = [x[1] for x in options]
            if property_type == 'options':
                return list(zip(list(map(lambda x: ', '.join(map(str, x)), options)), values))
            elif property_type == 'value':
                return values

    def get_sims(self, value):
        return list(self.load_yaml(value).keys())

    def get_scenario_sims(self, value):
        scenario_base = "/home/jovyan/work/lmax_analytics_notebooks/notebooks/backtesting_strategies/config/scenarios"
        scenario_simulation_yaml = os.path.join(scenario_base, value, 'simulations.yaml')
        return list(self.load_yaml(scenario_simulation_yaml).keys())

    def update_scenario_sims(self, *args):
        self.w['scenario_sims'].options = self.get_scenario_sims(self.w['scenario'].value)
        self.w['scenario_sims'].value = self.get_scenario_sims(self.w['scenario'].value)

    def update_simulations_sims(self, *args):
        self.w['sims'].options = self.get_sims(self.w['simulations_config'].value)
        self.w['sims'].value = self.get_sims(self.w['simulations_config'].value)

    def update_pipeline_config_files(self, *args):
        self.w['pipeline_config'].options = self.get_pipeline_config(self.w['filesystem'].value)
        self.w['pipeline_config'].value = self.get_pipeline_config(self.w['filesystem'].value)[0][1]

    def update_output_config_files(self, *args):
        self.w['output_config'].options = self.get_output_config(self.w['filesystem'].value)
        self.w['output_config'].value = self.get_output_config(self.w['filesystem'].value)[0][1]

    def update_simulations_config_files(self, *args):
        self.w['simulations_config'].options = self.get_simulations_config(self.w['filesystem'].value)
        self.w['simulations_config'].value = self.get_simulations_config(self.w['filesystem'].value)[0][1]

    def update_account_list_files(self, *args):
        self.w['accounts'].options = self.get_accounts(self.w['filesystem'].value)
        self.w['accounts'].value = self.get_accounts(self.w['filesystem'].value)[0][1]

    def update_instruments(self, *args):
        self.w['instruments'].options = self.get_instruments(self.w['shard'].value, property_type='options')
        self.w['instruments'].value = self.get_instruments(self.w['shard'].value, property_type='value')

    def update_pipeline_config(self, *args):
        if self.w['load_pipeline_config_from_file'].value == True:
            # turn on
            self.w['pipeline_config'].disabled = False
            # turn off
            self.w['shard'].disabled = True
            self.w['uid'].disabled = True
            self.w['lmax_account'].disabled = True
            self.w['matching_engine'].disabled = True
            self.w['event_stream'].disabled = True
            self.w['exclusion_window'].disabled = True
            self.w['level'].disabled = True
            self.w['netting_engine'].disabled = True
            self.w['calculate_cumulative_daily_pnl'].disabled = True

        elif self.w['load_pipeline_config_from_file'].value == False:
            # turn on
            self.w['shard'].disabled = False
            self.w['uid'].disabled = False
            self.w['lmax_account'].disabled = False
            self.w['matching_engine'].disabled = False
            self.w['event_stream'].disabled = False
            self.w['exclusion_window'].disabled = False
            self.w['level'].disabled = False
            self.w['netting_engine'].disabled = False
            self.w['calculate_cumulative_daily_pnl'].disabled = False
            # turn off
            self.w['pipeline_config'].disabled = True

    def update_simulations_config(self, *args):
        if self.w['load_simulation_config_from_file'].value == True:
            # set value
            self.w['load_instruments_from_snapshot'].value = False
            self.w['load_position_limits_from_snapshot'].value = False
            # turn on
            self.w['sims'].disabled = False
            self.w['simulations_config'].disabled = False
            # turn off
            self.w['simulation_reference'].disabled = True
            self.w['strategy_parameters'].disabled = True
            self.w['exit_parameters'].disabled = True
            self.w['event_filter_string'].disabled = True
            self.w['instruments'].disabled = True
            self.w['load_instruments_from_snapshot'].disabled = True
            self.w['load_position_limits_from_snapshot'].disabled = True
            self.w['load_target_accounts_from_snapshot'].disabled = True
            self.w['filter_snapshot_for_strategy'].disabled = True
            self.w['filter_snapshot_for_traded_account_instrument_pairs'].disabled = True

        elif self.w['load_simulation_config_from_file'].value == False:
            # turn on
            self.w['simulation_reference'].disabled = False
            self.w['strategy_parameters'].disabled = False
            self.w['exit_parameters'].disabled = False
            self.w['event_filter_string'].disabled = False
            self.w['instruments'].disabled = False
            self.w['load_instruments_from_snapshot'].disabled = False
            self.w['load_position_limits_from_snapshot'].disabled = False
            self.w['load_target_accounts_from_snapshot'].disabled = False
            self.w['filter_snapshot_for_strategy'].disabled = False
            self.w['filter_snapshot_for_traded_account_instrument_pairs'].disabled = False
            # turn off
            self.w['sims'].disabled = True
            self.w['simulations_config'].disabled = True

    def update_output_config(self, *args):
        if self.w['load_output_config_from_file'].value == True:
            # update value
            self.w['save'].value = False
            # turn on
            self.w['output_config'].disabled = False
            # turn off
            self.w['metrics'].disabled = True
            self.w['resample_rule'].disabled = True
            self.w['save'].disabled = True
            self.w['mode'].disabled = True
        elif self.w['load_output_config_from_file'].value == False:
            # turn on
            # self.w['metrics'].disabled = False
            self.w['resample_rule'].disabled = False
            self.w['save'].disabled = False
            # self.w['mode'].disabled = False
            # turn off
            self.w['output_config'].disabled = True

    def update_scenario_config(self, *args):
        if self.w['load_scenario'].value == True:
            # update value
            self.w['load_pipeline_config_from_file'].value = False
            self.w['load_simulation_config_from_file'].value = False
            self.w['load_output_config_from_file'].value = False
            self.w['load_instruments_from_snapshot'].value = False
            self.w['load_position_limits_from_snapshot'].value = False
            self.w['save'].value = False

            # turn on
            self.w['scenario'].disabled = False
            self.w['scenario_sims'].disabled = False

            # turn off
            self.w['load_pipeline_config_from_file'].disabled = True
            self.w['load_simulation_config_from_file'].disabled = True
            self.w['load_output_config_from_file'].disabled = True

            self.w['pipeline_config'].disabled = True

            self.w['shard'].disabled = True
            self.w['uid'].disabled = True
            self.w['lmax_account'].disabled = True
            self.w['matching_engine'].disabled = True
            self.w['event_stream'].disabled = True
            self.w['exclusion_window'].disabled = True
            self.w['level'].disabled = True
            self.w['netting_engine'].disabled = True
            self.w['calculate_cumulative_daily_pnl'].disabled = True

            self.w['sims'].disabled = True
            self.w['simulations_config'].disabled = True

            self.w['simulation_reference'].disabled = True
            self.w['strategy_parameters'].disabled = True
            self.w['exit_parameters'].disabled = True
            self.w['event_filter_string'].disabled = True
            self.w['instruments'].disabled = True
            self.w['load_instruments_from_snapshot'].disabled = True
            self.w['load_position_limits_from_snapshot'].disabled = True
            self.w['load_target_accounts_from_snapshot'].disabled = True
            self.w['filter_snapshot_for_strategy'].disabled = True
            self.w['filter_snapshot_for_traded_account_instrument_pairs'].disabled = True

            self.w['output_config'].disabled = True

            self.w['metrics'].disabled = True
            self.w['resample_rule'].disabled = True
            self.w['save'].disabled = True
            self.w['mode'].disabled = True

        if self.w['load_scenario'].value == False:
            # turn off
            self.w['scenario'].disabled = True
            self.w['scenario_sims'].disabled = True

            # turn on
            self.w['load_pipeline_config_from_file'].disabled = False
            self.w['load_simulation_config_from_file'].disabled = False
            self.w['load_output_config_from_file'].disabled = False

            self.w['shard'].disabled = False
            self.w['uid'].disabled = False
            self.w['lmax_account'].disabled = False
            self.w['matching_engine'].disabled = False
            self.w['event_stream'].disabled = False
            self.w['exclusion_window'].disabled = False
            self.w['level'].disabled = False
            self.w['netting_engine'].disabled = False
            self.w['calculate_cumulative_daily_pnl'].disabled = False
            self.w['simulation_reference'].disabled = False
            self.w['strategy_parameters'].disabled = False
            self.w['exit_parameters'].disabled = False
            self.w['event_filter_string'].disabled = False
            self.w['instruments'].disabled = False
            self.w['load_instruments_from_snapshot'].disabled = False
            self.w['load_position_limits_from_snapshot'].disabled = False
            self.w['load_target_accounts_from_snapshot'].disabled = False
            self.w['filter_snapshot_for_strategy'].disabled = False
            self.w['filter_snapshot_for_traded_account_instrument_pairs'].disabled = False
            self.w['metrics'].disabled = False
            self.w['resample_rule'].disabled = False
            self.w['save'].disabled = False
            # self.w['mode'].disabled = False

    def update_save_widgets(self, *args):
        if self.w['save'].value == True:
            self.w['mode'].disabled = False
        elif self.w['save'].value == False:
            self.w['mode'].disabled = True

    def update_metric_widgets(self, *args):
        if self.w['resample_rule'].value != None:
            self.w['metrics'].disabled = False
        elif self.w['resample_rule'].value == None:
            self.w['metrics'].disabled = True

    def update_instrument_widgets(self, *args):
        if self.w['load_instruments_from_snapshot'].value == False:
            self.w['instruments'].disabled = False
        elif self.w['load_instruments_from_snapshot'].value == True:
            self.w['instruments'].disabled = True

    def display(self):
        # WIDGETS
        w = {}

        # PIPELINE CONFIG
        self.w['load_pipeline_config_from_file'] = widgets.Checkbox(value=False,
                                                                    description='load Pipeline Config from file',
                                                                    indent=False)
        p1 = widgets.Label(value='select pipeline config file')
        self.w['pipeline_config'] = widgets.Dropdown(options=self.get_pipeline_config('local'),
                                                     value=self.get_pipeline_config('local')[0][1], disabled=True)
        p2 = widgets.Label(value='set shard')
        self.w['shard'] = widgets.Dropdown(options=['ldprof', 'nyprof', 'typrof'], value='ldprof')
        p3 = widgets.Label(value='set unique identifier')
        self.w['uid'] = widgets.Text(value='ex1', placeholder='enter a unique identifier', disabled=False)
        p4 = widgets.Label(value='set account id')
        self.w['lmax_account'] = widgets.IntText(value=1463064262, disabled=False)
        p5 = widgets.Label(value='set matching engine')
        self.w['matching_engine'] = widgets.Dropdown(
            options=[('fully fill TOB qty', 'matching_engine_default'), ('fill at TOB qty', 'matching_engine_tob'),
                     ('apply slippage', 'matching_engine_distribution')],
            value='matching_engine_default',
        )
        p6 = widgets.Label(value='set event stream type')
        self.w['event_stream'] = widgets.Dropdown(
            options=[('stratified sampling (0.3)', 'event_sample_stream'),
                     ('time snapshot (sec)', 'event_stream_snapshot')],
            value='event_sample_stream',
        )
        p7 = widgets.Label(value='set exclusion window {HH:MM - HH:MM}')
        self.w['exclusion_window'] = widgets.Text(value='16:30 - 18:30', placeholder='[[HH,MM], [HH, MM]]',
                                                  disabled=False)
        p8 = widgets.Label(value='set level')
        self.w['level'] = widgets.Dropdown(
            options=[('mark to market', 'mark_to_market'), ('trades only', 'trades_only')], value='mark_to_market')
        p9 = widgets.Label(value='set netting engine')
        self.w['netting_engine'] = widgets.Dropdown(options=[('FIFO', 'fifo')], value='fifo')
        self.w['calculate_cumulative_daily_pnl'] = widgets.Checkbox(value=True,
                                                                    description='calculate cumulative daily PnL',
                                                                    indent=False)

        # SIMULATION CONFIG
        self.w['load_simulation_config_from_file'] = widgets.Checkbox(value=False,
                                                                      description='load Simulation Config from file',
                                                                      indent=False)
        s1 = widgets.Label(value='select simulations config file')
        self.w['simulations_config'] = widgets.Dropdown(options=self.get_simulations_config('local'),
                                                        value=self.get_simulations_config('local')[0][1], disabled=True)
        s2 = widgets.Label(value='simulation reference')
        self.w['simulation_reference'] = widgets.Text(value='simulation', placeholder='enter a simulation reference',
                                                      disabled=False)
        s3 = widgets.Label(value='set strategy parameters (JSON)')
        self.w['strategy_parameters'] = widgets.VBox(
            [widgets.Textarea(layout={'height': '100%'},
                              value=STRATEGY_PARAMETERS_DEFAULT)], layout={'height': '224px'})
        s4 = widgets.Label(value='set exit parameters (JSON)')
        self.w['exit_parameters'] = widgets.VBox(
            [widgets.Textarea(layout={'height': '100%'},
                              value=EXIT_PARAMETERS_DEFAULT)], layout={'height': '100px'})
        s5 = widgets.Label(value='set instruments')
        self.w['instruments'] = widgets.SelectMultiple(options=self.get_instruments(self.w['shard'].value, property_type='options'),
                                                       value=self.get_instruments(self.w['shard'].value, property_type='value'), rows=13,
                                                       disabled=False)
        s6 = widgets.Label(value='set event filter string')
        self.w['event_filter_string'] = widgets.Text(placeholder='account_id != {enter account_id}', disabled=False)

        self.w['filter_snapshot_for_strategy'] = widgets.Checkbox(value=False,
                                                                  description='filter snapshot for strategy',
                                                                  indent=False)
        self.w['filter_snapshot_for_traded_account_instrument_pairs'] = widgets.Checkbox(value=False,
                                                                                         description='filter by traded account, instrument pairs',
                                                                                         indent=False)
        self.w['load_instruments_from_snapshot'] = widgets.Checkbox(value=False,
                                                                    description='load instruments from snapshot',
                                                                    indent=False)
        self.w['load_position_limits_from_snapshot'] = widgets.Checkbox(value=False,
                                                                    description='load position limits from snapshot',
                                                                    indent=False)
        self.w['load_target_accounts_from_snapshot'] = widgets.Checkbox(value=False,
                                                                        description='load target accounts from snapshot',
                                                                        indent=False)

        # OUTPUT CONFIG
        self.w['load_output_config_from_file'] = widgets.Checkbox(value=False,
                                                                  description='load Output Config from file',
                                                                  indent=False)
        o1 = widgets.Label(value='select output config file')
        self.w['output_config'] = widgets.Dropdown(options=self.get_output_config('local'),
                                                   value=self.get_output_config('local')[0][1], disabled=True)

        o2 = widgets.Label(value='set metrics')
        self.w['metrics'] = widgets.SelectMultiple(options=[('performance overview', 'performance_overview'),
                                                            ('trading actions breakdown', 'trading_actions_breakdown'),
                                                            ('inventory overview', 'inventory_overview')],
                                                   value=['performance_overview', 'trading_actions_breakdown',
                                                          'inventory_overview'], disabled=True)

        o3 = widgets.Label(value='set resample rule')
        self.w['resample_rule'] = widgets.Dropdown(
            options=[('None', None), ('1H', '1H'), ('summary (trading session)', 'summary')])
        self.w['save'] = widgets.Checkbox(value=False, description='save to s3', indent=False)
        o4 = widgets.Label(value='set write mode')
        self.w['mode'] = widgets.Dropdown(options=[('Overwrite', 'w'), ('Append', 'a')], disabled=True)

        # COMMAND LINE CONFIG
        c1 = widgets.Label(value='set start date (REQUIRED)')
        self.w['start_date'] = widgets.DatePicker(disabled=False)
        c2 = widgets.Label(value='set end date (REQUIRED)')
        self.w['end_date'] = widgets.DatePicker(disabled=False)
        c3 = widgets.Label(value='set target accounts (OPTIONAL)')
        self.w['accounts'] = widgets.Dropdown(options=self.get_accounts('local'))
        c4 = widgets.Label(value='set filesystem to load config from')
        self.w['filesystem'] = widgets.Dropdown(options=['s3', 'local'], value='local', disabled=False)
        c5 = widgets.Label(value='set a subset of simulations to run from the scenario')
        self.w['load_scenario'] = widgets.Checkbox(value=False, description='run bactest from scenario', indent=False)
        c8 = widgets.Label(value='scenario to execute')
        self.w['scenario'] = widgets.Dropdown(options=os.listdir(
            "/home/jovyan/work/lmax_analytics_notebooks/notebooks/backtesting_strategies/config/scenarios"),
                                              value=os.listdir(
                                                  "/home/jovyan/work/lmax_analytics_notebooks/notebooks/backtesting_strategies/config/scenarios")[
                                                  0],
                                              disabled=True)
        self.w['scenario_sims'] = widgets.SelectMultiple(options=self.get_scenario_sims(self.w['scenario'].value),
                                                         value=self.get_scenario_sims(self.w['scenario'].value), rows=5,
                                                         disabled=True)
        c6 = widgets.Label(value='set no of cores')
        self.w['cores'] = widgets.IntSlider(value=5, min=0, max=10, step=1, disabled=False, continuous_update=False,
                                            orientation='horizontal', readout=True, readout_format='d')
        c7 = widgets.Label(value='set no of batches')
        self.w['batch'] = widgets.IntSlider(
            value=1, min=0, max=20, step=1, disabled=False, continuous_update=False, orientation='horizontal',
            readout=True, readout_format='d')

        c9 = widgets.Label(value='set subset of simulations to run')
        self.w['sims'] = widgets.SelectMultiple(options=self.get_sims(self.w['simulations_config'].value),
                                                value=self.get_sims(self.w['simulations_config'].value), rows=5,
                                                disabled=True)

        # INTERACTIONS
        self.w['filesystem'].observe(self.update_pipeline_config_files, 'value')
        self.w['filesystem'].observe(self.update_simulations_config_files, 'value')
        self.w['filesystem'].observe(self.update_output_config_files, 'value')
        self.w['filesystem'].observe(self.update_account_list_files, 'value')
        self.w['shard'].observe(self.update_instruments, 'value')
        self.w['load_pipeline_config_from_file'].observe(self.update_pipeline_config, 'value')
        self.w['load_simulation_config_from_file'].observe(self.update_simulations_config, 'value')
        self.w['load_output_config_from_file'].observe(self.update_output_config, 'value')
        self.w['save'].observe(self.update_save_widgets, 'value')
        self.w['resample_rule'].observe(self.update_metric_widgets, 'value')
        self.w['load_instruments_from_snapshot'].observe(self.update_instrument_widgets, 'value')
        self.w['load_scenario'].observe(self.update_scenario_config, 'value')
        self.w['simulations_config'].observe(self.update_simulations_sims, 'value')
        self.w['scenario'].observe(self.update_scenario_sims, 'value')

        # WIDGET BOXES
        ui1 = widgets.VBox(
            [
                widgets.Label(value='COMMAND LINE CONFIG'),
                c1, self.w['start_date'],
                c2, self.w['end_date'],
                c3, self.w['accounts'],
                c4, self.w['filesystem'],
                c6, self.w['cores'],
                c7, self.w['batch'],
                widgets.Label(value=''),
                widgets.Label(value='SCENARIO CONFIG'),
                self.w['load_scenario'],
                c8, self.w['scenario'],
                c5, self.w['scenario_sims']
            ], height=1000)
        ui2 = widgets.VBox(
            [
                widgets.Label(value='PIPELINE CONFIG'),
                self.w['load_pipeline_config_from_file'],
                p1, self.w['pipeline_config'],
                widgets.Label(value=''),
                widgets.Label(value=''),
                widgets.Label(value=''),
                p2, self.w['shard'],
                p3, self.w['uid'],
                p4, self.w['lmax_account'],
                p5, self.w['matching_engine'],
                p6, self.w['event_stream'],
                p7, self.w['exclusion_window'],
                p8, self.w['level'],
                p9, self.w['netting_engine'],
                self.w['calculate_cumulative_daily_pnl']

            ], height=1000)
        ui3 = widgets.HBox([widgets.VBox(
            [
                widgets.Label(value='SIMULATION CONFIG'),
                self.w['load_simulation_config_from_file'],
                s1, self.w['simulations_config'],
                widgets.Label(value=''),
                widgets.Label(value=''),
                widgets.Label(value=''),
                s2, self.w['simulation_reference'],
                s3, self.w['strategy_parameters'],
                s4, self.w['exit_parameters']
            ], height=1000),
            widgets.VBox(
                [
                    widgets.Label(value=''), widgets.Label(value=''), c9, self.w['sims'],
                    widgets.Label(value=''),
                    s6, self.w['event_filter_string'],
                    s5, self.w['instruments'],
                    self.w['load_instruments_from_snapshot'],
                    self.w['load_target_accounts_from_snapshot'],
                    self.w['load_position_limits_from_snapshot'],
                    self.w['filter_snapshot_for_strategy'],
                    self.w['filter_snapshot_for_traded_account_instrument_pairs']
                ])
        ])
        ui4 = widgets.VBox(
            [
                widgets.Label(value='OUTPUT CONFIG'),
                self.w['load_output_config_from_file'],
                o1, self.w['output_config'],
                widgets.Label(value=''),
                widgets.Label(value=''),
                widgets.Label(value=''),
                o3, self.w['resample_rule'],
                o2, self.w['metrics'],
                self.w['save'],
                o4, self.w['mode']
            ], height=1000)

        # APPLICATION
        app = AppLayout(header=widgets.Label(
            value='RISK STRATEGY BACKTESTING: for guidance on running backtest please see the README.md in the same directory as this notebooks'),
                        left_sidebar=None,
                        center=widgets.HBox([ui1, ui2, ui3, ui4]),
                        right_sidebar=None,
                        footer=None,
                        pane_widths=[2, 5, 2],
                        pane_heights=[.3, 5, 5])
        return app