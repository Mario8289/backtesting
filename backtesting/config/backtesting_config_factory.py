import logging
import os
from io import BytesIO
from typing import Union, Dict, List, Any

import pandas as pd
import s3fs
import yaml

from risk_backtesting.config.backtesting_config import BackTestingConfig
from risk_backtesting.filesystems import (
    FILESYSTEM_TYPE_LOCAL,
    FILESYSTEM_TYPE_S3,
    initialise_s3fs,
)

FILE_TYPE_YAML: str = "yaml"
FILE_TYPE_CSV: str = "csv"


class BackTestingConfigFactory:
    def __init__(self):
        self.logger = logging.getLogger("BackTestingConfigBuilder")
        self.loaders: Dict = {
            FILE_TYPE_YAML: self._load_yaml,
            FILE_TYPE_CSV: self._load_csv,
        }
        self.pipeline_path: str = ""
        self.simulations_config_path: str = ""
        self.output_config_path: str = ""
        self.target_account_path: str = ""

    def build(
            self,
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
            colour: str,
            nomad: bool,
    ) -> BackTestingConfig:
        if not bucket:
            bucket = os.getenv("BUCKET_NAME")

        if not num_cores:
            num_cores = int(os.getenv("BACKTESTING_CORES", "5"))

        auth: Dict[str, Dict[str, Any]] = self._build_auth(
            auth_path, minio_uri, dataserver_uri, colour, nomad
        )

        self.configure_paths(
            filesystem_type,
            bucket,
            scenario_path,
            scenario,
            pipeline_path,
            simulations_path,
            output_path,
            target_accounts_path,
        )

        return self._build(
            auth,
            initialise_s3fs(auth, "config"),
            num_cores,
            filesystem_type,
            num_batches,
            start_date,
            end_date,
            simulations_filter,
        )

    def configure_paths(
            self,
            filesystem_type: str,
            bucket: str,
            scenario_path: str,
            scenario: str,
            pipeline_path: str,
            simulations_path: str,
            output_path: str,
            target_accounts_path: str,
    ):
        if scenario:
            if not scenario_path:
                raise ValueError(
                    "Scenario Path must be provided when specifying scenario name"
                )
            if not scenario_path.startswith("/"):
                raise ValueError("Scenario Path must be absolute (start with /)")
            base_path: str = f"{scenario_path}/{scenario}/"
            self.pipeline_path = self._build_path(
                filesystem_type, bucket, "pipeline.yaml", base=base_path
            )
            self.simulations_config_path = self._build_path(
                filesystem_type, bucket, "simulations.yaml", base=base_path
            )
            self.output_config_path = self._build_path(
                filesystem_type, bucket, "output.yaml", base=base_path
            )
            self.target_account_path = self._build_path(
                filesystem_type, bucket, "target_accounts.csv", base=base_path
            )

        if pipeline_path:
            self.pipeline_path = self._build_path(
                filesystem_type, bucket, pipeline_path
            )
        if simulations_path:
            self.simulations_config_path = self._build_path(
                filesystem_type, bucket, simulations_path
            )
        if output_path:
            self.output_config_path = self._build_path(
                filesystem_type, bucket, output_path
            )
        if target_accounts_path:
            self.target_account_path = self._build_path(
                filesystem_type, bucket, target_accounts_path
            )

        if (
                not self.pipeline_path
                or not self.simulations_config_path
                or not self.output_config_path
        ):
            raise ValueError(
                "Not all required config paths (pipeline/simulations/output) were specified."
            )

    def _build_auth(
            self,
            auth_path: str,
            minio_uri: str,
            dataserver_uri: str,
            colour: str,
            nomad: bool,
    ) -> Dict[str, Dict[str, Any]]:
        if nomad:
            self.logger.info("building auth from nomad style source")
            nomad_auth: Dict[str, Any] = self._load_file(
                auth_path, filesystem_type=FILESYSTEM_TYPE_LOCAL, default={}
            )
            if not minio_uri:
                minio_uri = os.getenv("BUCKET_HOST")
                if not minio_uri:
                    minio_uri = nomad_auth["uris"]["minio"]["export"][colour]
            if not dataserver_uri:
                dataserver_uri = os.getenv("DATASERVER_HOST")
                if not dataserver_uri:
                    dataserver_uri = nomad_auth["uris"]["dataserver"][colour]
            key: str = os.getenv("AWS_ACCESS_KEY_ID")
            if not key:
                key = nomad_auth["creds"]["minio"]["export"]["key"]
            secret: str = os.getenv("AWS_SECRET_ACCESS_KEY")
            if not secret:
                secret = nomad_auth["creds"]["minio"]["export"]["secret"]
            token: str = os.getenv("DATASERVER_TOKEN")
            if not token:
                token = nomad_auth["creds"]["dataserver"]["token"]

            auth: Dict[str, Dict[str, Any]] = {
                "input": {"minio": {"key": key, "secret": secret, "uri": minio_uri}},
                "output": {"minio": {"key": key, "secret": secret, "uri": minio_uri}},
                "config": {"minio": {"key": key, "secret": secret, "uri": minio_uri}},
                "dataserver": {"token": token, "uri": dataserver_uri},
            }
        else:
            self.logger.info("building auth")
            auth: Dict[str, Dict[str, Any]] = self._load_file(
                auth_path, filesystem_type=FILESYSTEM_TYPE_LOCAL, default={}
            )
            if minio_uri:
                for sect in auth.values():
                    sect.get("minio", {})["uri"] = minio_uri
            if dataserver_uri:
                auth["dataserver"]["uri"] = dataserver_uri
        return auth

    @staticmethod
    def _build_path(
            filesystem_type: str, bucket: str, file: str, base: str = "/"
    ) -> str:
        if FILESYSTEM_TYPE_S3 == filesystem_type:
            return f"{bucket}{base}{file}"
        if file.startswith(base):
            return file
        return f"{base}{file}"

    def _build(
            self,
            auth: Dict[str, Dict[str, Any]],
            configfs: s3fs.S3FileSystem,
            num_cores: int,
            filesystem_type: str,
            num_batches: int,
            start_date: str,
            end_date: str,
            simulations_filter: List[str],
    ) -> BackTestingConfig:
        self.logger.info("build config")

        pipeline: Dict[str, Any] = self._load_file(
            self.pipeline_path, filesystem_type, configfs
        )
        simulations: Dict[str, Any] = self._load_file(
            self.simulations_config_path, filesystem_type, configfs
        )
        output: Dict[str, Any] = self._load_file(
            self.output_config_path, filesystem_type, configfs
        )
        target_accounts: pd.DataFrame = self._load_file(
            self.target_account_path, filesystem_type, configfs,
        )

        config: BackTestingConfig = BackTestingConfig(auth, None, pipeline, output)
        config.optionally_override_running_config_parameters(
            start_date, end_date, num_cores, num_batches
        )
        config.optionally_set_target_accounts(target_accounts)
        config.build_simulations_config(simulations, simulations_filter)
        config.validate()

        return config

    def _load_file(
            self,
            file: str,
            filesystem_type: str,
            filesystem=None,
            default: Union[Dict, pd.DataFrame] = None,
    ) -> Union[Dict, pd.DataFrame]:
        if file:
            loader = self.loaders[file.split(".")[-1]]
            return loader(file, filesystem_type, filesystem, default)
        return default

    def _load_yaml(
            self, file: str, filesystem_type: str, filesystem=None, default: Dict = None,
    ) -> Dict:
        if FILESYSTEM_TYPE_S3 == filesystem_type:
            if filesystem is not None:
                with filesystem.open(file, mode="rb", refresh=True) as inf:
                    return yaml.safe_load(inf.read())
            else:
                self.logger.warning(
                    f"Could not load from {filesystem_type}:{file} (fs: {filesystem}"
                )
        elif FILESYSTEM_TYPE_LOCAL == filesystem_type:
            with open(file, "r") as inf:
                return yaml.safe_load(inf)
        else:
            self.logger.warning(f"Unknown filesystem {filesystem_type} for {file}")
        return default

    def _load_csv(
            self,
            file: str,
            filesystem_type: str,
            filesystem=None,
            default: pd.DataFrame = None,
    ) -> pd.DataFrame:
        if FILESYSTEM_TYPE_S3 == filesystem_type:
            if filesystem is not None:
                if filesystem.exists(file):
                    with filesystem.open(file, mode="rb", refresh=True) as inf:
                        return pd.read_csv(BytesIO(inf.read()))
            else:
                self.logger.warning(
                    f"Could not load from {filesystem_type}:{file} (fs: {filesystem}"
                )
        elif FILESYSTEM_TYPE_LOCAL == filesystem_type:
            if os.path.exists(file):
                return pd.read_csv(file)
        else:
            self.logger.warning(f"Unknown filesystem {filesystem_type} for {file}")
        return default
