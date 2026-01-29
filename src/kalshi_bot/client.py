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

    def create_order(self, ticker, side, action, count, price=None, order_type="limit") -> dict:
        """Place an order.

        For limit orders, price (1-99 cents) is required.
        For market orders, price is ignored — Kalshi fills at best available.
        """
        kwargs = {
            "ticker": ticker,
            "side": side,
            "action": action,
            "count": count,
            "type": order_type,
        }

        # Only set price for limit orders — market orders must not include price
        if order_type != "market" and price is not None:
            if side == "yes":
                kwargs["yes_price"] = price
            else:
                kwargs["no_price"] = price

        req = CreateOrderRequest(**kwargs)

        resp = self._orders_api.create_order_without_preload_content(
            create_order_request=req
        )
        data = json.loads(resp.data)
        return data.get("order", data)
