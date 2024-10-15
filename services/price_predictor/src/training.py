from typing import Optional

from loguru import logger
from sklearn.metrics import mean_absolute_error

from src.config import HopsworksConfig


def train_model(
    hopsworks_config: HopsworksConfig,
    feature_view_name: str,
    feature_view_version: int,
    feature_group_name: str,
    feature_group_version: int,
    ohlc_window_sec: int,
    product_id: str,
    last_n_days: int,
    forecast_steps: int,
    perc_test_data: Optional[float] = 0.3,
):
    """
    Reads features from the Feature Store
    Trains a predictive model,
    Saves the model to the model registry

    Args:
        hopsworks_config (HopsworksConfig): The Hopsworks configuration
        feature_view_name (str): The name of the feature view in the feature store
        feature_view_version (int): The version of the feature view in the feature store
        feature_group_name (str): The name of the feature group in the feature store
        feature_group_version (int): The version of the feature group in the feature store
        ohlc_window_sec (int): The window size in seconds for the OHLC data
        product_id (str): The product ID for which we want to train the model
        last_n_days (int): The number of days to go back in time to get the training data
        forecast_steps (int): The number of steps to forecast into the future
        perc_test_data (Optional[float]): The percentage of data to use for testing

    Returns:
        None
    """
    # Load feature data from the Feature store
    from src.ohlc_data_reader import OhlcDataReader

    ohlc_data_reader = OhlcDataReader(
        ohlc_window_sec=ohlc_window_sec,
        hopsworks_config=hopsworks_config,
        feature_view_name=feature_view_name,
        feature_view_version=feature_view_version,
        feature_group_name=feature_group_name,
        feature_group_version=feature_group_version,        
    )

    # Read the sorted data from the offline store
    # data is sorted by timestamp_ms
    ohlc_data = ohlc_data_reader.read_from_offline_store(
        product_id=product_id,
        last_n_days=last_n_days,
    )
    logger.debug(f"Read {len(ohlc_data)} rows from the offline store")

    # Split the data into training and testing
    logger.debug(f"Splitting the data into training and testing with {perc_test_data} test data")
    test_size = int(perc_test_data * len(ohlc_data))
    train_df = ohlc_data.iloc[:-test_size]
    test_df = ohlc_data.iloc[-test_size:]
    logger.debug(f"Train data shape: {train_df.shape}")
    logger.debug(f"Test data shape: {test_df.shape}")


    # Add a column with the target price we want our model to predict
    # for both the training and testing data
    train_df['target_price'] = train_df['close'].shift(-forecast_steps)
    test_df['target_price'] = test_df['close'].shift(-forecast_steps)
    logger.debug(f'Added target price column to the data')

    # Remove rows with NaN values
    train_df = train_df.dropna()
    test_df = test_df.dropna()
    logger.debug(f'Removed rows with NaN values')
    logger.debug(f'Train data shape after removing NaNs: {train_df.shape}')
    logger.debug(f'Test data shape after removing NaNs: {test_df.shape}')

    # Split the data into features and target
    X_train = train_df.drop(columns=['target_price'])
    y_train = train_df['target_price']
    X_test = test_df.drop(columns=['target_price'])
    y_test = test_df['target_price']
    logger.debug(f'Split the data into features and target')

    # Log dimensions of the features and target
    logger.debug(f'X_train shape: {X_train.shape}')
    logger.debug(f'y_train shape: {y_train.shape}')
    logger.debug(f'X_test shape: {X_test.shape}')
    logger.debug(f'y_test shape: {y_test.shape}')

    # Build a model
    from src.models.current_price_baseline import CurrentPriceBaseline

    model = CurrentPriceBaseline()
    model.fit(X_train, y_train)
    logger.debug(f'Fitted the model')

    # Evaluate the model
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    logger.debug(f'Mean Absolute Error: {mae}')

    # Push the model to the model registry



if __name__ == '__main__':
    
    from src.config import config, hopsworks_config

    train_model(
        hopsworks_config=hopsworks_config,
        feature_view_name=config.feature_view_name,
        feature_view_version=config.feature_view_version,
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version,
        ohlc_window_sec=config.ohlc_window_sec,
        product_id=config.product_id,
        last_n_days=config.last_n_days,
        forecast_steps=config.forecast_steps,
    )