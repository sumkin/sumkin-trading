import numpy as np
import statsmodels.formula.api as smf
import statsmodels.stats.api as sms
import statsmodels.tsa.stattools as ts
from scipy.stats import ttest_1samp
import pymannkendall as mk

class CointegrationPairChecker:

    def __init__(self, df, params):
        self.df = df
        self.params = params

    def cointegrate(self):
        fit = smf.ols("close2 ~ close1", data=self.df).fit()

        # Check volume in money. It should not be less than 1 million for both stocks.
        money_volume1 = np.mean(self.df["volume1"] * self.df["close1"])
        money_volume2 = np.mean(self.df["volume2"] * self.df["close2"])
        if (money_volume1 < self.params["MIN_MONEY_VOLUME"] or
            money_volume2 < self.params["MIN_MONEY_VOLUME"]):
            return False, None, None, None, {}

        # Prices should not differ more than 10 times.
        mean_price1 = self.df["close1"].mean()
        mean_price2 = self.df["close2"].mean()
        mean_price_ratio = mean_price2 / mean_price1
        if (mean_price_ratio < self.params["MIN_MEAN_PRICE_RATIO"] or
            mean_price_ratio > self.params["MAX_MEAN_PRICE_RATIO"]):
            return False, None, None, None, {}

        # Limit hedge ratio.
        hedge_ratio = fit.params["close1"]
        intercept = fit.params["Intercept"]
        if (hedge_ratio < self.params["MIN_HEDGE_RATIO"] or
            hedge_ratio > self.params["MAX_HEDGE_RATIO"]):
            return False, None, None, None, {}

        # Test for stationarity of residuals.
        p_val_adfuller = ts.adfuller(fit.resid)[1]
        if p_val_adfuller > self.params["MAX_ADFULLER"]:
            return False, None, None, None, {}

        # Test for homoscedasticity.
        p_val_bp = sms.het_breuschpagan(fit.resid, fit.model.exog)[1]
        if p_val_bp < self.params["MIN_HOMOSCED_P_VAL"]:
            return False, None, None, None, {}

        # Test for zero mean.
        p_val_zm = ttest_1samp(fit.resid, 0.0).pvalue
        if p_val_zm < self.params["MIN_ZERO_MEAN_P_VAL"]:
            return False, None, None, None, {}

        # Standard deviation of residuals should not be small.
        if np.std(fit.resid) < self.params["MIN_STD_RESID"]:
            return False, None, None, None, {}

        # Test for no-trend in residuals (Mann-Kendall).
        mk_res = mk.original_test(fit.resid)
        if mk_res.h:
            return False, None, None, None, {}

        info = {
            "p_val_adfuller": p_val_adfuller,
            "p_val_bp": p_val_bp,
            "p_val_zm": p_val_zm,
            "mk_trend": mk_res.trend,
            "hedge_ratio": hedge_ratio
        }

        return True, hedge_ratio, intercept, fit.resid, info



