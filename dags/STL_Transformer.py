from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
from datetime import timedelta, datetime
from joblib import dump, load
from sklearn.preprocessing import StandardScaler, MaxAbsScaler, RobustScaler
from utils.logger import Logger
from scipy.stats import boxcox
from statsmodels.tsa.seasonal import seasonal_decompose, STL
from matplotlib import pyplot
import numpy as np
from scipy.interpolate import CubicSpline
from scipy.signal import find_peaks


class All_Positive_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self):  # no *args or **kargs
        self.logger = Logger(name=__name__)

    def fit(self, time_series, y=None):
        return self  # nothing else to do

    def transform(self, data):
        time_series = data.copy()
        try:
            for col_name, col_value in time_series.iteritems():
                min_value = time_series[col_name].min()
                if min_value < 0:
                    time_series[col_name] = time_series[col_name] - min_value
                time_series[time_series[col_name] == 0] = time_series[time_series[col_name] == 0] + 0.001
            
            return time_series
        except Exception as error:
            self.logger.error(
                "Encounter an error during transform raw data {} to positive value:{}".format(col_name, error))
            return None


class STL_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self):  # no *args or **kargs
        self.logger = Logger(name=__name__)

    def fit(self, time_series, y=None):
        return self  # nothing else to do

    def transform(self, time_series, lmbda=None, if_seasonal=True, period=None):

        for col in time_series:
            try:
                array, lamb = boxcox(time_series[col])
                time_series[col + '_BOX_COX'] = array
                if if_seasonal:
                    result = STL(time_series[col], period=period).fit()
                    time_series[col + '_trend'] = result.trend
                    time_series[col + '_seasonal'] = result.seasonal
                else:
                    cs = CubicSpline(x=time_series.index, y=time_series[col])
                    time_series[col + '_trend'] = cs(time_series.index, 3)
                    time_series[col + '_seasonal'] = 0
                time_series[col + '_de-Trend'] = time_series[col + '_BOX_COX'] - time_series[col + '_trend']
                time_series[col + '_de-Seasonal'] = time_series[col + '_BOX_COX'] - time_series[col + '_seasonal']
                pyplot.show()
            except (ValueError, Exception) as error:
                self.logger.error(error)
                return None
        return time_series


class Periodicity_Transformer(BaseEstimator, TransformerMixin):
    def __init__(self):  # no *args or **kargs
        self.logger = Logger(name=__name__)

    def fit(self, time_series, y=None):
        return self  # nothing else to do

    def autocorr(self, x, lag):
        '''numpy.corrcoef, partial'''
        corr = 1. if lag == 0 else np.corrcoef(x[lag:], x[:-lag])[0][1]
        print(corr)
        return corr

    def transform(self, data):
        for col in data:
            try:
                lag = int(len(data.index) / 3)
                print(lag)
                x = self.autocorr(data[col], lag)
                return data
            except (ValueError, Exception) as error:
                self.logger.error(error)
                return None
