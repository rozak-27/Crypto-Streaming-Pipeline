from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import json

@dataclass
class PriceTick:
    """Representasi satu tick data harga crypto."""
    timestamp: str    # ISO 8601 format
    symbol: str       # BTC-USD, ETH-USD, dll
    price: float
    bid: float
    ask: float
    volume: float
    source: str = "simulator"

    @classmethod
    def create(cls, symbol: str, price: float,
               bid: float, ask: float, volume: float) -> "PriceTick":
        return cls(
            timestamp=datetime.now(timezone.utc).isoformat(),
            symbol=symbol,
            price=price, bid=bid, ask=ask, volume=volume,
        )

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())