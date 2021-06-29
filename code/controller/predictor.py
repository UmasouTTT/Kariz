import pandas as pd
import config as cfg
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn import metrics
from sklearn.linear_model import Lasso



class Predictor:
    def __init__(self):
        # read the csv file
        self.pig_stats = pd.read_csv(cfg.pig_runtime_stats)
        self.pig_regressors = self.build_pig_predictor()
        self.spark_stats = pd.read_csv(cfg.spark_runtime_stats)
        self.spark_regressors =self.build_spark_predictor()

        self.cache_block_sz = 4 * 1024 * 1024
        self.bw_cache = 5 * (10 ** 9)

        pass

    def pig_predic(self, feature):
        pass

    def build_pig_predictor(self):
        df2 = self.prepare_pig_data()
        gdf2 = df2.groupby('feature')
        regressors = {}
        for index, df3 in gdf2:
            X = df3[['remote_read', 'cache_read', 'sequential', 'shuffle', 'aggregate']].values
            Y = df3['runtime'].values
            X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=0)

            regressor = Lasso(alpha=0.0001, precompute=True, max_iter=1000,
                              positive=True, random_state=9999, selection='random')
            regressor.fit(X_train, y_train)
            if index not in regressors:
                regressors[index] = regressor;
        return regressors

    def build_spark_predictor(self):
        pass

    def prepare_pig_data(self):
        self.pig_stats = self.pig_stats.apply(self.bandwidth_regularization, axis=1)
        df2 = pd.DataFrame()
        df2['feature'] = self.pig_stats['Feature']
        df2['input_sz'] = self.pig_stats['total_bytes']
        df2['cache_read'] = self.pig_stats['prefetched_blocks'] / self.bw_cache
        df2['remote_read'] = np.abs(8*(self.pig_stats['total_remote_bytes'] - self.pig_stats['prefetched_blocks']) / (self.pig_stats['bw_i']  * (10 ** 9)))
        df2['sequential'] = self.pig_stats['total_bytes'] + self.pig_stats['total_remote_bytes']
        df2['aggregate'] = np.log(self.pig_stats['Maps'] + self.pig_stats['Reduces'])
        df2['shuffle'] = (self.pig_stats['Maps'] + self.pig_stats['Reduces'])  # *dt['total_bytes']
        df2['runtime'] = self.pig_stats['runtime']
        return df2

    def pig_bandwidth_regularization(row):
        if 'Gbps' in row['bw']:
            row['bw_i'] = float(row['bw'].replace(r'Gbps', ''))
        elif 'Mbps' in row['bw']:
            row['bw_i'] = int(row['bw'].replace(r'Mbps', ''))/1000
        else:
            raise NameError('The bandwidth string is not parsable')
        return row
