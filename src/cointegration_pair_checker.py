import numpy as np
import statsmodels.formula.api as smf
import statsmodels.stats.api as sms
import statsmodels.tsa.stattools as ts
from scipy.stats import ttest_1samp

class CointegrationPairChecker:

    def __init__(self, df):
        self.df = df

    def cointegrate(self):
        fit = smf.ols("close2 ~ close1", data=self.df).fit()

        # Check volume in money. It should not be less than 1 million for both stocks.
        money_volume1 = np.mean(self.df["volume1"] * self.df["close1"])
        money_volume2 = np.mean(self.df["volume2"] * self.df["close2"])
        if money_volume1 < 10e6 or money_volume2 < 10e6:
            return False, None, None, None

        # Prices should not differ more than 10 times.
        mean_price1 = self.df["close1"].mean()
        mean_price2 = self.df["close2"].mean()
        mean_price_ratio = mean_price2 / mean_price1
        if mean_price_ratio < 0.1 or mean_price_ratio > 10:
            return False, None, None, None

        # Limit hedge ratio.
        hedge_ratio = fit.params["close1"]
        intercept = fit.params["Intercept"]
        if abs(hedge_ratio) > 5 or abs(hedge_ratio) < 0.2:
            return False, None, None, None

        # Test for stationarity of residuals.
        p_val_adfuller = ts.adfuller(fit.resid)[1]
        if p_val_adfuller > 0.005:
            return False, None, None, None

        # Test for homoscedasticity.
        p_val_bp = sms.het_breuschpagan(fit.resid, fit.model.exog)[1]
        if p_val_bp < 0.02:
            return False, None, None, None

        # Test for zero mean.
        p_val_zm = ttest_1samp(fit.resid, 0.0).pvalue
        if p_val_zm < 0.01:
            return False, None, None, None

        return True, hedge_ratio, intercept, fit.resid



