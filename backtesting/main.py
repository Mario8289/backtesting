import logging
import os
import sys
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, Namespace
from typing import List, Dict, Any

import yaml

from risk_backtesting.config.backtesting_config import BackTestingConfig
from risk_backtesting.config.backtesting_config_factory import (
    BackTestingConfigFactory,
    FILESYSTEM_TYPE_LOCAL,
    FILESYSTEM_TYPE_S3,
)
from risk_backtesting.risk_backtesting_result import (
    BackTestingResults,
    build_backtesting_results,
)
from risk_backtesting.simulation_runner import SimulationRunner


def parse_args() -> Namespace:
    parser: ArgumentParser = ArgumentParser(
        formatter_class=ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "-a", "--auth", dest="auth", help="Path to auth.yaml",
    )
    parser.add_argument(
        "--minio", help="LMAX min.io server URI (overrides any value in --auth)",
    )
    parser.add_argument(
        "--dataserver", help="LMAX dataserver URI (overrides any value in --auth)",
    )

    parser.add_argument(
        "-r",
        "--cores",
        help="Number of CPU cores to use. Default: 1. Overrides whatever is in --config",
        type=int,
    )

    parser.add_argument(
        "-f",
        "--filesystem",
        help="filesystem type",
        choices=[FILESYSTEM_TYPE_LOCAL, FILESYSTEM_TYPE_S3],
        default=FILESYSTEM_TYPE_LOCAL,
    )
    parser.add_argument(
        "-u",
        "--bucket",
        help=f"Bucket for config location (if using a {FILESYSTEM_TYPE_S3} filesystem)",
    )

    parser.add_argument("--scenario_path", help="Path to scenario directory")
    parser.add_argument(
        "--scenario", help="Which scenario to load the configuration from"
    )

    parser.add_argument(
        "-p", "--pipeline", dest="pipeline", help="Path to pipeline configuration yaml",
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

    parser.add_argument(
        "-l",
        "--simulations_config",
        dest="simulations_config",
        help="Path to simulation configuration yaml",
    )

    parser.add_argument(
        "-o",
        "--output_config",
        dest="output_config",
        help="Path to output configuration yaml",
    )

    parser.add_argument(
        "-t",
        "--target_account",
        help="Path to target account csv (overrides any value in --pipeline)",
    )

    parser.add_argument("--log_level", help="What log level to use")

    return parser.parse_args()


def main(
        auth_path: str,
        minio_uri: str,
        dataserver_uri: str,
        filesystem_type: str,
        bucket: str,
        scenario_path: str,
        scenario: str,
        pipeline_path: str,
        simulations_path: str,
        output_path: str,
        target_accounts_path: str,
        num_cores: int,
        num_batches: int,
        start_date: str,
        end_date: str,
        simulations_filter: List[str],
        return_results: bool,
        colour: str = None,
        nomad: bool = False,
) -> BackTestingResults:
    config: BackTestingConfig = BackTestingConfigFactory().build(
        auth_path,
        minio_uri,
        dataserver_uri,
        filesystem_type,
        bucket,
        scenario_path,
        scenario,
        pipeline_path,
        simulations_path,
        output_path,
        target_accounts_path,
        num_cores,
        num_batches,
        start_date,
        end_date,
        simulations_filter,
        colour,
        nomad,
    )

    results: BackTestingResults = build_backtesting_results(return_results)

    SimulationRunner().run(config, results)

    return results


# noinspection PyBroadException
def load_yaml(path: str, default: Dict[str, Any] = None) -> Dict[str, Any]:
    if path:
        if os.path.exists(path):
            try:
                with open(path, "r") as inf:
                    return yaml.safe_load(inf)
            except Exception:
                pass
    return default


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
    env_nomad: str = os.getenv("BACKTESTING_NOMAD", "NO")
    nomad: bool = "YES" == env_nomad
    colour: str = os.getenv("BACKTESTING_COLOUR") if nomad else None
    try:
        results: BackTestingResults = main(
            args.auth,
            args.minio,
            args.dataserver,
            args.filesystem,
            args.bucket,
            args.scenario_path,
            args.scenario,
            args.pipeline,
            args.simulations_config,
            args.output_config,
            args.target_account,
            args.cores,
            int(args.batch) if args.batch is not None else None,
            args.start_date,
            args.end_date,
            [] if args.sims is None else args.sims.split(","),
            False,
            colour,
            nomad,
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
