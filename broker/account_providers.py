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
        self.api_key = api_key or os.getenv("ALPACA_API_KEY") or os.getenv("API_KEY")
        self.api_secret = api_secret or os.getenv("ALPACA_API_SECRET") or os.getenv("SECRET_KEY")
        self.client = TradingClient(self.api_key, self.api_secret, paper=paper)

    def get_snapshot(self, fields: List[str] = None) -> Dict[str, any]:
        """
        Returns a dictionary containing the requested account fields.
        If no fields are specified, return all fields.
        """
        account = self.client.get_account()

        # account_dict = account.__dict__
        account_dict = dict(account)

        if fields:
            snapshot = {k: account_dict.get(k) for k in fields}
        else:
            snapshot = account_dict

        print("snapshot", snapshot)
        return snapshot
