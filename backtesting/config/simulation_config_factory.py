from datetime import date
from itertools import product
from typing import Dict, Any, Set, List

import pandas as pd

from risk_backtesting.config.backtesting_output_config import BackTestingOutputConfig
from risk_backtesting.config.simulation_config import SimulationConfig

SIMULATION_DEFAULTS: Dict[str, Any] = {
    "exit_parameters": {"exit_type": "exit_default"},
    "risk_parameters": {"risk_type": "no_risk"},
}


class SimulationConfigFactory:
    @staticmethod
    def build_simulation_configs(
            simulations: Dict[str, Any],
            simulations_filter: List[str],
            lmax_account: int,
            uid: str,
            version: int,
            shard: str,
            start_date: date,
            end_date: date,
            load_starting_positions: bool,
            load_client_starting_positions: bool,
            calculate_cumulative_daily_pnl: bool,
            level: str,
            output: BackTestingOutputConfig,
            target_accounts: pd.DataFrame,
    ) -> Dict[str, SimulationConfig]:
        configs: Dict[str, Any] = simulations
        if simulations_filter:
            configs = {k: v for k, v in simulations.items() if k in simulations_filter}
        simulations: Dict[str, SimulationConfig] = {}
        for simulation_label, simulation_params in configs.items():
            # set simulations defaults
            params: Dict[
                str, Any
            ] = SimulationConfigFactory._ensure_simulation_default_params(
                simulation_params
            )

            load_accounts_from_snapshot: bool = params.get(
                "load_target_accounts_from_snapshot", False
            )

            load_instruments_from_snapshot: bool = params.get(
                "load_instruments_from_snapshot", False
            )

            load_position_limits_from_snapshot: bool = params.get(
                "load_position_limits_from_snapshot", False
            )

            load_booking_risk_from_snapshot: bool = params.get(
                "load_booking_risk_from_snapshot", False
            )

            load_booking_risk_from_target_accounts: bool = params.get(
                "load_booking_risk_from_target_accounts", False
            )

            load_internalisation_risk_from_snapshot: bool = params.get(
                "load_internalisation_risk_from_snapshot", False
            )

            load_internalisation_risk_from_target_accounts: bool = params.get(
                "load_internalisation_risk_from_target_accounts", False
            )

            filter_snapshot_for_strategy: bool = params.get(
                "filter_snapshot_for_strategy", True
            )

            filter_snapshot_for_traded_account_instrument_pairs: bool = params.get(
                "filter_snapshot_for_traded_account_instrument_pairs", False
            )

            relative_simulation: Dict[str, Any] = params.get("relative_simulation", {})

            is_relative_simulation: bool = relative_simulation is not None and len(
                relative_simulation
            ) > 0
            # options in|out
            relative_simulation_direction: str = relative_simulation.get("direction")
            relative_comparison_accounts: List[int] = relative_simulation.get(
                "comparison_accounts"
            )
            # options client|internalisation
            relative_comparison_accounts_type: str = relative_simulation.get(
                "comparison_accounts_type"
            )

            split_by_instrument: bool = params.get("split_by_instrument", True)

            # need to construct the unique simulations if a simulation context has more than one plan
            if any(
                    [
                        type(x) == list
                        for x in list(params["strategy_parameters"].values())
                                 + list(params["exit_parameters"].values())
                                 + list(params["risk_parameters"].values())
                    ]
            ):
                unique_sims = SimulationConfigFactory._build_list_of_simulations(
                    params.get("constructor"),
                    list(params["strategy_parameters"].values()),
                    list(params["exit_parameters"].values()),
                    list(params["risk_parameters"].values()),
                )

                for i, sim in enumerate(unique_sims):
                    idx_strat = len(params["strategy_parameters"].keys())
                    idx_exit = len(params["exit_parameters"].keys())
                    strategy_params = {
                        x[0]: x[1]
                        for x in zip(
                            params["strategy_parameters"].keys(), sim[:idx_strat]
                        )
                        if x[1] is not None
                    }
                    exit_params = {
                        x[0]: x[1]
                        for x in zip(
                            params["exit_parameters"].keys(),
                            sim[idx_strat : idx_strat + idx_exit],
                        )
                        if x[1] is not None
                    }

                    risk_params = {
                        x[0]: x[1]
                        for x in zip(
                            params["risk_parameters"].keys(),
                            sim[idx_strat + idx_exit :],
                        )
                        if x[1] is not None
                    }

                    sim_label = f"{simulation_label}_{i + 1}"

                    strategy_params["account_id"] = lmax_account
                    strategy_params["instruments"] = params["instruments"]

                    # todo: cintezam refactor the instrument and account id assignation
                    simulations[sim_label] = SimulationConfig(
                        name=simulation_label,
                        uid=uid,
                        version=version,
                        shard=shard,
                        start_date=start_date,
                        end_date=end_date,
                        load_starting_positions=load_starting_positions,
                        load_client_starting_positions=load_client_starting_positions,
                        load_target_accounts_from_snapshot=load_accounts_from_snapshot,
                        load_instruments_from_snapshot=load_instruments_from_snapshot,
                        load_position_limits_from_snapshot=load_position_limits_from_snapshot,
                        load_booking_risk_from_snapshot=load_booking_risk_from_snapshot,
                        load_internalisation_risk_from_snapshot=load_internalisation_risk_from_snapshot,
                        load_booking_risk_from_target_accounts=load_booking_risk_from_target_accounts,
                        load_internalisation_risk_from_target_accounts=load_internalisation_risk_from_target_accounts,
                        filter_snapshot_for_strategy=filter_snapshot_for_strategy,
                        filter_snapshot_for_traded_account_instrument_pairs=filter_snapshot_for_traded_account_instrument_pairs,
                        relative_simulation=is_relative_simulation,
                        relative_simulation_direction=relative_simulation_direction,
                        relative_comparison_accounts=relative_comparison_accounts,
                        relative_comparison_accounts_type=relative_comparison_accounts_type,
                        calculate_cumulative_daily_pnl=calculate_cumulative_daily_pnl,
                        level=level,
                        strategy_parameters=strategy_params,
                        exit_parameters=exit_params,
                        risk_parameters=risk_params,
                        instruments=None
                        if load_instruments_from_snapshot
                        else params.get("instruments"),
                        output=output,
                        event_filter_string=params.get("event_filter_string"),
                        target_accounts=None
                        if load_accounts_from_snapshot
                        else target_accounts,
                        split_by_instrument=split_by_instrument,
                    )
            else:

                strategy_parameters: Dict[str, Any] = params["strategy_parameters"]
                strategy_parameters["account_id"] = lmax_account
                strategy_parameters["instruments"] = params.get("instruments", None)

                simulations[simulation_label] = SimulationConfig(
                    name=simulation_label,
                    uid=uid,
                    version=version,
                    shard=shard,
                    start_date=start_date,
                    end_date=end_date,
                    load_starting_positions=load_starting_positions,
                    load_client_starting_positions=load_client_starting_positions,
                    load_target_accounts_from_snapshot=load_accounts_from_snapshot,
                    load_instruments_from_snapshot=load_instruments_from_snapshot,
                    load_position_limits_from_snapshot=load_position_limits_from_snapshot,
                    load_booking_risk_from_snapshot=load_booking_risk_from_snapshot,
                    load_internalisation_risk_from_snapshot=load_internalisation_risk_from_snapshot,
                    load_booking_risk_from_target_accounts=load_booking_risk_from_target_accounts,
                    load_internalisation_risk_from_target_accounts=load_internalisation_risk_from_target_accounts,
                    filter_snapshot_for_strategy=filter_snapshot_for_strategy,
                    filter_snapshot_for_traded_account_instrument_pairs=filter_snapshot_for_traded_account_instrument_pairs,
                    relative_simulation=is_relative_simulation,
                    relative_simulation_direction=relative_simulation_direction,
                    relative_comparison_accounts=relative_comparison_accounts,
                    relative_comparison_accounts_type=relative_comparison_accounts_type,
                    calculate_cumulative_daily_pnl=calculate_cumulative_daily_pnl,
                    level=level,
                    strategy_parameters=strategy_parameters,
                    exit_parameters=params["exit_parameters"],
                    risk_parameters=params["risk_parameters"],
                    instruments=None
                    if load_instruments_from_snapshot
                    else params.get("instruments"),
                    output=output,
                    event_filter_string=params.get("event_filter_string"),
                    target_accounts=None
                    if load_accounts_from_snapshot
                    else target_accounts,
                    split_by_instrument=split_by_instrument,
                )

        return simulations

    @staticmethod
    def _ensure_simulation_default_params(
            simulation_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        keys: Set[str] = set(simulation_params.keys())
        keys.update(SIMULATION_DEFAULTS.keys())
        return {k: simulation_params.get(k, SIMULATION_DEFAULTS.get(k)) for k in keys}

    @staticmethod
    def _build_list_of_simulations(
            transform_type: str,
            strategy_parameters: list,
            exit_parameters: list,
            risk_parameters: list,
    ):
        parameters: list = strategy_parameters + exit_parameters + risk_parameters
        if "zip" == transform_type:
            # ensure that each property is a list of the same size
            no_of_sims = list(set([len(x) for x in parameters if type(x) == list]))
            if len(no_of_sims) > 1:
                raise IndexError(
                    "All simulation properties of type list must be the same length"
                )

            padded_parameters = [
                [x for i in range(0, no_of_sims[0])] if type(x) != list else x
                for x in parameters
            ]

            return list(zip(*padded_parameters))
        elif "product" == transform_type:
            # ensure that each property is a list and contains unique_values
            list_parameters = [
                list(set(x)) if type(x) == list else [x] for x in parameters
            ]

            return list(product(*list_parameters))
        else:
            raise ValueError(f"{transform_type} is not a valid simulation constructor")
