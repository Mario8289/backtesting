from typing import AnyStr, Dict, List, Any, Union
import logging
import os
import pandas as pd
from pathlib import Path

from backtesting.datastore import get_datastore
from backtesting.config.backtesting_config import BackTestingConfig

FILE_TYPE_YAML: str = "yaml"
FILE_TYPE_CSV: str = "csv"


class BackTestingConfigFactory:
    def __init__(
            self,
            datastore,

    ):
        self.logger = logging.getLogger("BackTestingConfigBuilder")
        self.datastore = datastore
        self.loaders: Dict = {
            FILE_TYPE_YAML: self.datastore.load_yaml,
            FILE_TYPE_CSV: self.datastore.load_csv,
        }
        self.subscriptions_path: AnyStr = ""
        self.pipeline_path: str = ""
        self.simulations_config_path: str = ""
        self.output_config_path: str = ""
        self.target_account_path: str = ""

    @classmethod
    def create(
            cls,
            datastore,
            datastore_parameters
    ):
        _attributes = datastore_parameters
        _auth = _attributes.get('auth', {})

        _datastore = get_datastore(datastore)

        datastore = _datastore.create({k: v for (k, v) in _attributes.items() if k != 'auth'})

        datastore.authenticate(
            auth=_auth
        )

        return cls(datastore=datastore)

    def build(
            self,
            scenario: AnyStr,
            scenario_path: AnyStr,
            subscriptions_path: AnyStr,
            pipeline_path: AnyStr,
            simulations_path: AnyStr,
            output_path: AnyStr,
            num_cores: int,
            num_batches: int,
            start_date: AnyStr,
            end_date: AnyStr,
            simulations_filter: List[AnyStr]
    ) -> BackTestingConfig:

        if not num_cores:
            num_cores = int(os.getenv("BACKTESTING_CORES", "5"))

        self.configure_paths(
            scenario=scenario,
            scenario_path=scenario_path,
            subscriptions_path=subscriptions_path,
            pipeline_path=pipeline_path,
            simulations_path=simulations_path,
            output_path=output_path,
        )

        return self._build(
            num_cores=num_cores,
            num_batches=num_batches,
            start_date=start_date,
            end_date=end_date,
            simulations_filter=simulations_filter
        )

    def configure_paths(
            self,
            scenario: AnyStr,
            scenario_path: AnyStr,
            subscriptions_path: AnyStr,
            pipeline_path: AnyStr,
            simulations_path: AnyStr,
            output_path: AnyStr
    ):

        if scenario:
            if not scenario_path:
                raise ValueError(
                    "Scenario Path must be provided when specifying scenario name"
                )
            if not scenario_path.startswith("/"):
                raise ValueError("Scenario Path must be absolute (start with /)")

            base_path: str = (Path(scenario_path) / scenario).as_posix()

            self.pipeline_path = self.datastore.build_path(
                file="pipeline.yaml", base=base_path
            )
            self.simulations_config_path = self.datastore.build_path(
                file="simulations.yaml", base=base_path
            )
            self.subscriptions_path = self.datastore.build_path(
                file="subscriptions.yaml", base=base_path
            )
            self.output_config_path = self.datastore.build_path(
                file="output.yaml", base=base_path
            )

        if pipeline_path:
            self.pipeline_path = self.datastore.build_path(
                base=pipeline_path
            )
        if simulations_path:
            self.simulations_config_path = self.datastore.build_path(
                base=simulations_path
            )

        if subscriptions_path:
            self.subscriptions_path = self.datastore.build_path(
                base=subscriptions_path
            )

        if output_path:
            self.output_config_path = self.datastore.build_path(
                base=output_path
            )

        if (
                not self.pipeline_path
                or not self.simulations_config_path
                or not self.subscriptions_path
                or not self.output_config_path
        ):
            raise ValueError(
                "Not all required config paths (pipeline/simulations/output) were specified."
            )

    def _build(
            self,
            num_cores: int,
            num_batches: int,
            start_date: str,
            end_date: str,
            simulations_filter: List[str],
    ) -> BackTestingConfig:
        self.logger.info("build config")

        pipeline: Dict[str, Any] = self._load_file(
            self.pipeline_path
        )
        _subscriptions: Dict[str, Any] = self._load_file(
            self.subscriptions_path
        )
        subscriptions: Dict[str, Any] = _subscriptions['subscriptions']
        subscriptions_cache: Dict[str, Any] = _subscriptions['subscriptions_cache']
        simulations: Dict[str, Any] = self._load_file(
            self.simulations_config_path
        )
        output: Dict[str, Any] = self._load_file(
            self.output_config_path
        )

        config: BackTestingConfig = BackTestingConfig(
            pipeline=pipeline,
            subscriptions=subscriptions,
            subscriptions_cache=subscriptions_cache,
            output=output
        )
        config.optionally_override_running_config_parameters(
            start_date=start_date,
            end_date=end_date,
            num_cores=num_cores,
            num_batches=num_batches
        )
        config.build_simulations_config(
            simulations=simulations,
            simulations_filter=simulations_filter
        )
        config.validate()

        return config

    def _load_file(
            self,
            file: str,
    ) -> Union[Dict, pd.DataFrame]:
        if file:
            loader = self.loaders[file.split(".")[-1]]
            output = loader(file)
        return output
