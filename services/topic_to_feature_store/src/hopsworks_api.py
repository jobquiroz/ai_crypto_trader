from typing import List
import hopsworks
from src.config import hopsworks_config as config
import pandas as pd

project = hopsworks.login(
        project=config.hopsworks_project_name,
        api_key_value=config.hopsworks_api_key
    )

feature_store = project.get_feature_store()


def push_value_to_feature_group(
    value: List[dict], 
    feature_group_name: str, 
    feature_group_version: int,
    feature_group_primary_keys: List[str],
    feature_group_event_time: str,
    start_offline_materialization: bool,
):
    """
    Pushes the given value to the given feature group in the feature store

    Args: 
        value (dict): The value to push to the feature group
        feature_group_name (str): The name of the feature group in the feature store
        feature_group_version (int): The version of the feature group in the feature store
        feature_group_primary_key (List[str]): The primary key of the feature group
        feature_group_event_time (str): The event time of the feature group
        start_offline_materialization (bool): Whether to start offline materialization or not
          when we save the data to the feature store

    Returns:
        None
    """

    
    feature_group = feature_store.get_or_create_feature_group(
        name = feature_group_name,
        version = feature_group_version,
        description = "Feature group for the incoming messages",
        primary_key = feature_group_primary_keys,
        event_time = feature_group_event_time,
        online_enabled = True,
        #expectation_suite = expectation_suite_transactions,
    )
    # Transform the value dict into a pandas 
    value_df = pd.DataFrame(value)

    # breakpoint()

    feature_group.insert(
        value_df, 
        write_options= {'start_offline_materialization': start_offline_materialization})