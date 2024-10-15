import pandas as pd

class CurrentPriceBaseline:

    def __init__(self):
        pass

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
        return X['close']
