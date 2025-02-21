import datetime as dt
import logging

import pandas as pd

from backtesting.writers import Writer


def construct_path(datasource_label: str, date: dt.date = None) -> str:
    if date is not None:
        year = date.strftime("%Y")
        month = date.strftime("%m")
        day = date.strftime("%d")
        path = "datasource_label={}/year={}/month={}/day={}".format(
            datasource_label, year, month, day
        )
    else:
        path = "datasource_label={}".format(datasource_label)

    return path


def construct_filename(*args, **kwargs):
    x = len(args)
    if kwargs.get("file") is None:
        if kwargs.get("date") is not None:
            fname = "{}-{}".format(kwargs.get("date"), kwargs.get("task"))
        else:
            fname = "{}".format(kwargs.get("task"))
        if x != 0:
            fname = "{}-{}".format(fname, args[0])
            while x > 0:
                x -= 1
                for arg in args[1:]:
                    fname = fname + "-" + str(arg)

        fname = "{}.csv".format(fname)
    else:
        fname = kwargs.get("file")
    return fname


def parse_groupby_cols(df, split_results_by, freq):
    split_results_by = (
        [split_results_by] if type(split_results_by) != list else split_results_by
    )
    for i, col in enumerate(split_results_by):
        if df.reset_index()[col].dtype.name in [
            "datetime64[ns]",
            "datetime64[ns, UTC]",
        ]:
            split_results_by[i] = pd.Grouper(key=col, freq=freq)
    return split_results_by


def save_simulation(
        writer: Writer,
        df: pd.DataFrame,
        uid: str,
        version: int,
        mode: str,
        store_index: bool,
        split_results_by: str = None,
        split_results_freq: str = None,
        file: str = None,
):
    logger = logging.getLogger("save")

    if df.shape[0] != 0:
        if split_results_by is not None:
            split_results_by = parse_groupby_cols(
                df, split_results_by, split_results_freq
            )
            grps = df.reset_index().groupby(by=split_results_by)
            for by, grp in grps:
                cols = [by] if type(by) != list else by
                date = (
                    [x for x in cols if isinstance(x, dt.datetime)][0]
                    if any([x for x in cols if isinstance(x, dt.datetime)])
                    else None
                )
                file = construct_filename(
                    *[x for x in cols if x != date],
                    date=date.date(),
                    task="simulation-output-{}-{}".format(uid, version),
                    file=file,
                )
                logger.info(file)

                logger.info(date)
                logger.info(mode)

                writer.write_results(
                    results=grp.copy(),
                    store_index=store_index,
                    mode=mode,
                    file=file,
                    date=date.date(),
                )

        else:
            datestr = dt.datetime.now().strftime("%Y-%m-%d")
            file = construct_filename(
                task="{}-simulation-output-{}-{}".format(datestr, uid, version),
                filename=file,
            )
            writer.write_results(
                results=df.copy(),
                store_index=store_index,
                mode=mode,
                file=file,
            )
