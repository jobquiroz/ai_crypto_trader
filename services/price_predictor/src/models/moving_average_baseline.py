import pandas as pd

class MovingAverageBaseline:
    """
    A simple moving average baseline model.
    """

    def __init__(self, window_size: int):
        self.window_size = window_size


    def fit(self, X: pd.DataFrame, y: pd.Series):
        """
        Fit the model to the training data.

        Args:
            X (pd.DataFrame): The training data.
            y (pd.Series): The target data.
        """
        pass

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """
        Predict the target variable for the given data.

        Args:
            X (pd.DataFrame): The data for which we want to make predictions.

        Returns:
            pd.Series: The predicted target variable.
        """
        raise NotImplementedError("Pending implementation")
        #return X['close'].rolling(window=10).mean().shift(1)