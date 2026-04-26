from alpaca.trading.client import TradingClient
from typing import List, Dict
import os
from dotenv import load_dotenv

load_dotenv(".env")

class AlpacaAccountProvider:
    """
    Fetches account data from Alpaca and provides snapshots
    for rule evaluation.
    """

    def __init__(self, api_key: str = None, api_secret: str = None, paper: bool = True):
        self.api_key = (
            api_key or 
            os.getenv("ALPACA_API_KEY") or 
            os.getenv("API_KEY") or 
            os.getenv("APCA_API_KEY_ID")
        )
        self.api_secret = (
            api_secret or 
            os.getenv("ALPACA_API_SECRET") or 
            os.getenv("SECRET_KEY") or 
            os.getenv("APCA_API_SECRET_KEY")
        )

        if not self.api_key or not self.api_secret:
            print("[ENGINE WARNING] Alpaca credentials missing! Rule Engine will fail to hydrate account data.")

        self.client = TradingClient(self.api_key, self.api_secret, paper=paper)
        
        # Performance Multiplexing/Caching
        self._cached_account = None
        self._last_fetch_time = 0
        self._cache_ttl_seconds = 5.0

    def get_snapshot(self, fields: List[str] = None) -> Dict[str, any]:
        """
        Returns a dictionary containing the requested account fields.
        Uses a 5s TTL cache to prevent redundant REST calls on every tick.
        """
        import time
        now = time.time()
        
        if self._cached_account is None or (now - self._last_fetch_time > self._cache_ttl_seconds):
            print(f"[ACCOUNT] Cache expired or empty. Fetching fresh snapshot from Alpaca... (Age: {now - self._last_fetch_time:.2f}s)")
            account = self.client.get_account()
            self._cached_account = dict(account)
            self._last_fetch_time = now
        
        account_dict = self._cached_account

        if fields:
            snapshot = {k: account_dict.get(k) for k in fields}
        else:
            snapshot = account_dict

        print("snapshot", snapshot)
        return snapshot
