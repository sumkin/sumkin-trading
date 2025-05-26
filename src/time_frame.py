from tinkoff.invest import CandleInterval

class TimeFrame:
    INTERVAL_1_MIN = 1
    INTERVAL_5_MIN = 2
    INTERVAL_15_MIN = 3
    INTERVAL_HOUR = 4
    INTERVAL_2_HOUR = 5
    INTERVAL_DAY = 6

    @staticmethod
    def get_tinkoff_interval(tf):
        if tf == TimeFrame.INTERVAL_1_MIN:
            return CandleInterval.CANDLE_INTERVAL_1_MIN
        elif tf == TimeFrame.INTERVAL_5_MIN:
            return CandleInterval.CANDLE_INTERVAL_5_MIN
        elif tf == TimeFrame.INTERVAL_HOUR:
            return CandleInterval.CANDLE_INTERVAL_HOUR
        elif tf == TimeFrame.INTERVAL_2_HOUR:
            return CandleInterval.CANDLE_INTERVAL_2_HOUR
        elif tf == TimeFrame.INTERVAL_DAY:
            return CandleInterval.CANDLE_INTERVAL_DAY

    @staticmethod
    def get_num_minutes(tf):
        if tf == TimeFrame.INTERVAL_1_MIN:
            return 1
        elif tf == TimeFrame.INTERVAL_5_MIN:
            return 5
        elif tf == TimeFrame.INTERVAL_15_MIN:
            return 15
        elif tf == TimeFrame.INTERVAL_HOUR:
            return 60
        elif tf == TimeFrame.INTERVAL_2_HOUR:
            return 120
        elif tf == TimeFrame.INTERVAL_DAY:
            return 1440
        else:
            assert False

    @staticmethod
    def get_resolution(tf):
        if tf == TimeFrame.INTERVAL_1_MIN:
            return "1m"
        elif tf == TimeFrame.INTERVAL_5_MIN:
            return "5m"
        elif tf == TimeFrame.INTERVAL_15_MIN:
            return "15m"
        elif tf == TimeFrame.INTERVAL_HOUR:
            return "1h"
        else:
            assert False