import json

from kalshi_python_sync import ApiClient, Configuration, MarketApi, OrdersApi, PortfolioApi
from kalshi_python_sync.auth import KalshiAuth
from kalshi_python_sync.models.create_order_request import CreateOrderRequest


def create_client(config: dict) -> "KalshiBotClient":
    """Create an authenticated Kalshi client from config dict."""
    cfg = Configuration(host=config["host"])
    api_client = ApiClient(configuration=cfg)

    with open(config["private_key_path"]) as f:
        private_key_pem = f.read()

    api_client.kalshi_auth = KalshiAuth(config["api_key_id"], private_key_pem)
    return KalshiBotClient(api_client)


class KalshiBotClient:
    """Thin wrapper that returns dicts to avoid SDK Pydantic validation issues."""

    def __init__(self, api_client: ApiClient):
        self._market_api = MarketApi(api_client)
        self._orders_api = OrdersApi(api_client)
        self._portfolio_api = PortfolioApi(api_client)

    def get_balance(self) -> dict:
        resp = self._portfolio_api.get_balance_without_preload_content()
        return json.loads(resp.data)

    def get_markets(self, limit=20, status="open") -> list:
        resp = self._market_api.get_markets_without_preload_content(
            limit=limit, status=status
        )
        data = json.loads(resp.data)
        return data.get("markets", [])

    def get_market(self, ticker: str) -> dict:
        resp = self._market_api.get_market_without_preload_content(ticker=ticker)
        data = json.loads(resp.data)
        return data.get("market", data)

    def get_all_markets(self, status="open") -> list:
        """Fetch all markets using cursor pagination."""
        all_markets = []
        cursor = None
        while True:
            kwargs = {"limit": 1000, "status": status}
            if cursor:
                kwargs["cursor"] = cursor
            resp = self._market_api.get_markets_without_preload_content(**kwargs)
            data = json.loads(resp.data)
            markets = data.get("markets", [])
            all_markets.extend(markets)
            cursor = data.get("cursor")
            if not cursor or not markets:
                break
        return all_markets

    def get_positions(self) -> list:
        """Fetch all positions using cursor pagination."""
        all_positions = []
        cursor = None
        while True:
            kwargs = {"limit": 1000}
            if cursor:
                kwargs["cursor"] = cursor
            resp = self._portfolio_api.get_positions_without_preload_content(**kwargs)
            data = json.loads(resp.data)
            positions = data.get("market_positions", [])
            all_positions.extend(positions)
            cursor = data.get("cursor")
            if not cursor or not positions:
                break
        return all_positions

    def get_market_candlesticks(self, ticker, series_ticker, start_ts, end_ts, period_interval=60) -> list:
        """Fetch candlestick data for a market.

        period_interval: 1 (1min), 60 (1h), or 1440 (1d).
        Returns list of candlestick dicts.
        """
        resp = self._market_api.get_market_candlesticks_without_preload_content(
            series_ticker=series_ticker,
            ticker=ticker,
            start_ts=start_ts,
            end_ts=end_ts,
            period_interval=period_interval,
        )
        data = json.loads(resp.data)
        return data.get("candlesticks", [])

    def batch_get_market_candlesticks(self, tickers, start_ts, end_ts, period_interval=60) -> dict:
        """Fetch candlestick data for multiple markets at once.

        tickers: list of market ticker strings (max 100).
        Returns dict mapping ticker -> list of candlestick dicts.
        """
        resp = self._market_api.batch_get_market_candlesticks_without_preload_content(
            market_tickers=",".join(tickers),
            start_ts=start_ts,
            end_ts=end_ts,
            period_interval=period_interval,
        )
        data = json.loads(resp.data)
        return data.get("candlesticks", {})

    def create_order(self, ticker, side, action, count, price=None, order_type="limit") -> dict:
        """Place an order.

        For limit orders, price (1-99 cents) is required.
        For "market" orders, we send an aggressive limit at 99c to fill
        immediately — Kalshi requires a price field on all orders.
        """
        # For market-style orders, use 99c (max valid) to fill at best available
        effective_price = price if price is not None else 99

        kwargs = {
            "ticker": ticker,
            "side": side,
            "action": action,
            "count": count,
            "type": order_type,
        }

        if side == "yes":
            kwargs["yes_price"] = effective_price
        else:
            kwargs["no_price"] = effective_price

        req = CreateOrderRequest(**kwargs)

        resp = self._orders_api.create_order_without_preload_content(
            create_order_request=req
        )
        data = json.loads(resp.data)
        return data.get("order", data)
