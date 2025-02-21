from typing import Dict, Any, AnyStr
import logging
import os
import sys

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, Namespace
from typing import List

from backtesting.config.backtesting_config import BackTestingConfig
from backtesting.config.backtesting_config_factory import (
    BackTestingConfigFactory,
)
from backtesting.backtesting_result import (
    build_backtesting_results, BackTestingResults
)
from backtesting.simulation_runner import SimulationRunner


def parse_args() -> Namespace:
    parser: ArgumentParser = ArgumentParser(
        formatter_class=ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "-r",
        "--cores",
        help="Number of CPU cores to use. Default: 1. Overrides whatever is in --config",
        type=int,
    )

    parser.add_argument(
        "-z"
        "--datastore",
        dest='datastore',
        help="name of the datastore all configuration is stored")

    parser.add_argument(
        "-y"
        "--datastore_parameters",
        dest='datastore_parameters',
        help="authentication details for the datastore, entry points, etc.")

    parser.add_argument(
        "-a"
        "--scenario_path",
        dest='scenario_path',
        help="Path to scenario directory")

    parser.add_argument(
        "-c",
        "--subscriptions",
        dest="subscriptions_path",
        help="Path to subscriptions directory",
    )

    parser.add_argument(
        "-o"
        "--scenario",
        dest='scenario',
        help="Which scenario to load the configuration from"
    )

    parser.add_argument(
        "-p",
        "--pipeline",
        dest="pipeline_path",
        help="Path to pipeline configuration yaml",
    )

    parser.add_argument(
        "-l",
        "--simulations_config",
        dest="simulations_config_path",
        help="Path to simulation configuration yaml",
    )

    parser.add_argument(
        "-o",
        "--output_config",
        dest="output_config_path",
        help="Path to output configuration yaml",
    )

    parser.add_argument(
        "-b",
        "--batch",
        help="Number of batches to split the simulations into (overrides any value in --pipeline). Default: 1",
        type=int,
    )
    parser.add_argument(
        "-i",
        "--sims",
        help="Subset of simulations to run. Expects a list of comma separated simulation labels",
    )
    parser.add_argument(
        "-s",
        "--start_date",
        help="Start Date override; %Y-%m-%d format (overrides any value in --pipeline)",
    )
    parser.add_argument(
        "-e",
        "--end_date",
        help="End Date override; %Y-%m-%d format; inclusive (overrides any value in --pipeline)",
    )

    parser.add_argument("--log_level", help="What log level to use")

    return parser.parse_args()


def main(
        datastore: AnyStr,
        datastore_parameters: Dict[Any, Any],
        scenario_path: Any,
        scenario: AnyStr,
        subscriptions_path: AnyStr,
        pipeline_path: AnyStr,
        simulations_path: AnyStr,
        output_path: AnyStr,
        num_cores: int,
        num_batches: int,
        start_date: AnyStr,
        end_date: AnyStr,
        simulations_filter: List[AnyStr],
        return_results: bool
) -> BackTestingResults:

    """

    :param scenario_datastore: The name of the datastore where all configuration files are stored.
    :param scenario_datastore_parameters: any authentication credentials required to access the configuration files.
    :param scenario_path: This is the name base directory for the scenario that you want to run.
    :param scenario: This is the name of the scenario that you want to run.
    :param subscriptions_path: (OPTIONAL) is the path of the base folder that holds all your subscription credentials that you want to include when building your events stream, if not selected it will be set using the scenario that you have selected.
    :param pipeline_path: (OPTIONAL) This is the path to the pipeline file tha you want to run, if not selected it will be set using the scenario that you have selected.
    :param simulations_path: (OPTIONAL) This is the path to the simulations file that you want to run, if not selected it will be set using the scenario that you have selected.
    :param output_path: (OPTIONAL) This is the path to the output file that you want to run, if not selected it will be set using the scenario that you have selected.
    :param num_cores: (OPTIONAL) This will set the number of cores you want to use when running the simulations, if not set it will use the cores specificed in the pipeline file.
    :param num_batches: (OPTIONAL) This will set the number of batches you want to use when running the simulations, if not set it will use the number of batches specified in the pipeline file.
    :param start_date: (OPTIONAL) This will set the start date you want to use when runnning the simulations, if not set it will use the start date specificed in the pipleline file.
    :param end_date: (OPTIONAL) This will set the end date you want to use when runnning the simulations, if not set it will use the end date specificed in the pipleline file.
    :param simulations_filter: (OPTIONAL) This will restrict the simulations that are specific in the simulations_path file to a subset of simulations.
    :param return_results: (OPTIONAL) This specifies whether or not you want to return results to the terminal where you executed the command.
    :return:
    """

    config: BackTestingConfig = BackTestingConfigFactory.create(
        datastore=datastore, datastore_parameters=datastore_parameters
    ).build(
        scenario=scenario,
        scenario_path=scenario_path,
        subscriptions_path=subscriptions_path,
        pipeline_path=pipeline_path,
        simulations_path=simulations_path,
        output_path=output_path,
        num_cores=num_cores,
        num_batches=num_batches,
        start_date=start_date,
        end_date=end_date,
        simulations_filter=simulations_filter,
    )

    results: BackTestingResults = build_backtesting_results(return_results)

    SimulationRunner().run(config, results)

    return results


if __name__ == "__main__":
    args: Namespace = parse_args()
    log_level: str = args.log_level if args.log_level else (
        os.getenv("BACKTESTING_LOG_LEVEL", "INFO")
    )

    logging.basicConfig(
        level=log_level, format="[%(asctime)s][%(name)s][%(levelname)s] %(message)s",
    )
    logger = logging.getLogger("main")
    logger.setLevel(log_level)

    try:

        results: BackTestingResults = main(
            datastore=args.datastore,
            datastore_parameters=eval(args.datastore_parameters),
            scenario_path=args.scenario_path,
            subscriptions_path=args.subscriptions_path,
            scenario=args.scenario,
            pipeline_path=args.pipeline_path,
            simulations_path=args.simulations_config_path,
            output_path=args.output_config_path,
            num_cores=args.cores,
            num_batches=int(args.batch) if args.batch is not None else None,
            start_date=args.start_date,
            end_date=args.end_date,
            simulations_filter=[] if args.sims is None else args.sims.split(","),
            return_results=False,
        )

        if 0 != len(results.errors):
            for error in results.errors:
                logger.error(f"{error}", exc_info=error.payload)
            logger.error("Encountered errors running the backtester, please check logs")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt")

    except Exception as ex:
        logger.exception("Unexpected exception: ", ex)
        sys.exit(1)

    finally:
        logger.info("Shutting down")
        # for cleanup of connections etc.

    logger.info("Exit")
