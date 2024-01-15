from bokeh.plotting import figure
import pandas as pd
from bokeh.models import (
    ColumnDataSource,
    Span,
    DatetimeTickFormatter,
    NumeralTickFormatter,
    HoverTool,
    CDSView,
    BooleanFilter, Legend)
from bokeh.palettes import plasma


class Visulisation:

    @staticmethod
    def style_sim_profile(p):
        p.yaxis.formatter = NumeralTickFormatter(format="$0")
        p.xaxis.major_label_orientation = "vertical"
        p.legend.location = "top_left"
        p.legend.background_fill_alpha = 0.8
        p.legend.click_policy = "hide"
        p.xaxis.formatter = DatetimeTickFormatter(days='%b %d')
        return p

    def plot_sim_profile(self, *args, **kwargs):
        '''
        This visulisation allow you to pass in an array of simulation to view their associated profiles
         based on a user defined y.
        :param simulation data:
            this is the data for each simulation ran and corresponding performance metrics that you wish to visulise.
        :type simulation data: ``pd.DataFrame()``
        :param y:
            this is the metric that you want to use as the y for profiles
        :type y: ``str``
        :param hash:
            these are the individual simulations that you want to generate a profile for
        :type hash: ``list``

        :kwargs:
            * *include_sltp* (``boolean``)
                to include circle glyth show when take profits and stop losses were executed.
            * *aggregate_profile (``boolean``)
            * *resample_rile* (``str``)
                if your simulation data is to dense then you can resample to a defined temporal period e.g `1H`
            * *plot_width* (`int`)
                plot width
            * *plot_height* (`int`)
                plot height
            * *title* (``str``)
                plot title
        :return:
            a figure object that you can then view by wrapping the object in a the bokeh.plotting show() method.

        :rtype: bokeh.plotting.figure

        :Example:

        >>> plot_sim_profile(df, y='rpnl_cum', hash=['sim_1', 'sim_2'], include_sltp=True)
        >>> plot_sim_profile(df, 'rpnl_cum', ['sim_1'], resample_rule='1D')

        .. note:: add notes for use
        .. todo:: things to add
        '''
        df = args[0].copy()
        y = args[1]
        hashes = args[2]
        include_sltp = kwargs.get('include_sltp')
        resample_rule = kwargs.get('resample_rule')
        aggregate_profile = kwargs.get('aggregate_profile')
        simulation_parameters = list(df.columns[list(df.columns).index('simulation'):])
        plot_width = kwargs.get('plot_width') if kwargs.get('plot_width') else 1200
        plot_height = kwargs.get('plot_height') if kwargs.get('plot_height') else 680

        if any([x not in list(df['hash'].unique()) for x in hashes]):
            raise KeyError(
                'referenced simulation are not present in the dataframe, avialable simuliations are {}'.format(
                    list(df['hash'].unique())))

        if aggregate_profile:
            df = df[df.hash.isin(hashes)]
            df.sort_index(inplace=True)
            df['rpnl_cum'] = df['rpnl'].cumsum()
            df['hash'] = 'aggregated_hash'
            hashes = ['aggregated_hash']

        if resample_rule is not None:
            if include_sltp:
                raise ValueError('Cannot view SL/TP on resample plot')
            try:
                if resample_rule == 'trade_date':
                    time_col = 'trade_date'
                    time_grouper = [time_col]
                else:
                    time_col = 'timestamp'
                    time_grouper = [pd.Grouper(key=time_col, freq=resample_rule)]
                df = df.reset_index().groupby(simulation_parameters + time_grouper).agg(
                    {'rpnl_cum': 'last', 'rpnl': 'sum', 'trade_qty': ['sum', 'count']}).reset_index().set_index(
                    time_col)
                df.index.name = 'timestamp'
                simulation_parameters.extend(['rpnl_cum', 'rpnl', 'trade_qty', 'trade_cnt'])
                df.columns = simulation_parameters
                df = df[~df[y].isnull()]
            except KeyError as e:
                raise KeyError("key {} is not in the dataframe, possible keys are {}".format(e, df.columns))
        else:
            simulation_parameters.extend(['rpnl_cum', 'rpnl'])

        tooltips = []
        for col in simulation_parameters:
            if col in ['rpnl_cum', 'rpnl']:
                coltip = (col, '@' + col + '{$ 0.00 a}')
            else:
                coltip = (col, '@{}'.format(col))
            tooltips.append(coltip)
        tooltips.extend([('timestamp', '@timestamp{%Y-%m-%d %H:%M:%S}')])

        hover = HoverTool(
            tooltips=tooltips,
            formatters={'timestamp': 'datetime'},
            mode='mouse')
        hover.formatters = {'timestamp': "datetime"}

        p = figure(plot_width=plot_width, plot_height=plot_height,
                   title="Simulation RPNL Profile",
                   x_axis_label="timestamp",
                   x_axis_type='datetime',
                   y_axis_label=y,
                   tools=[hover, 'save', 'pan', 'box_zoom', 'wheel_zoom', 'reset'])

        hline = Span(location=0, dimension='width', line_color='red',
                     line_dash='solid', line_width=1)
        p.renderers.extend([hline])

        colors = plasma(hashes.__len__() + 1)
        idx = 0
        legend_line_items = []
        legend_point_items = []

        for sim_hash in hashes:
            df_sim = df[df['hash'] == sim_hash].reset_index()
            cds = ColumnDataSource(df_sim)
            legend = '{}'.format(sim_hash)
            l = p.line("timestamp", y, line_width=2, source=cds, color=colors[idx])
            legend_line_items.append((legend, [l]))
            idx += 1

        if include_sltp:
            cds_all = ColumnDataSource(df)
            for col, color in zip(['SL_close_position', 'TP_close_position'], ['red', 'green']):
                srcfilter = [True if action_value == col else False for action_value in cds_all.data['action']]
                view = CDSView(sources=cds_all, filters=[BooleanFilter(srcfilter)])
                legend = 'action: {}'.format(col[:2])
                c = p.circle("timestamp", y, color=color, source=cds_all, view=view)
                legend_point_items.append((legend, [c]))

        legend = Legend(items=legend_point_items + legend_line_items, location=(0, -60))
        p.add_layout(legend, 'right')
        # legend.click_policy="mute"
        p = self.style_sim_profile(p)

        return p
