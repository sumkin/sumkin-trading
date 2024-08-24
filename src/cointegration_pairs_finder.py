import os
from mpire import WorkerPool
from datetime import datetime, timedelta

from time_frame import TimeFrame
from tinkoff_universe import TinkoffUniverse
from tinkoff_data_reader import TinkoffDataReader

class CointegrationPairsFinder:

    def __init__(self, tf: TimeFrame, start: datetime, end: datetime):
        self.tf = tf
        self.start = start
        self.end = end

    def _read_dfs(self):
        tu = TinkoffUniverse()
        shares = tu.get_shares()

        tdr = TinkoffDataReader()
        with WorkerPool(n_jobs=os.cpu_count()) as pool:
            f = lambda figi: tdr.get_bars_df(figi, self.tf, start, end)
            self.dfs = pool.map(f, [share[1] for share in shares])
            assert len(shares) == len(self.dfs)
        print(len(self.dfs))
        print(self.dfs[0].head(5))

if __name__ == "__main__":
    end = datetime.now()
    start = datetime.now() - timedelta(minutes=1000)
    cpf = CointegrationPairsFinder(TimeFrame.INTERVAL_1_MIN, start, end)
    cpf._read_dfs()