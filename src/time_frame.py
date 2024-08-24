from tinkoff.invest import CandleInterval

class TimeFrame:
    INTERVAL_1_MIN = 1
    INTERVAL_5_MIN = 2
    INTERVAL_HOUR = 3
    INTERVAL_DAY = 4

    @staticmethod
    def get_tinkoff_interval(tf):
        if tf == TimeFrame.INTERVAL_1_MIN:
            return CandleInterval.CANDLE_INTERVAL_1_MIN
        elif tf == TimeFrame.INTERVAL_5_MIN:
            return CandleInterval.CANDLE_INTERVAL_5_MIN
        elif tf == TimeFrame.INTERVAL_HOUR:
            return CandleInterval.CANDLE_INTERVAL_HOUR
        elif tf == TimeFrame.INTERVAL_DAY:
            return CandleInterval.CANDLE_INTERVAL_DAY


