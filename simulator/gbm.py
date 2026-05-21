import numpy as np
from dataclasses import dataclass
from typing import Dict

@dataclass
class GBMConfig:
    """Konfigurasi parameter GBM per symbol."""
    initial_price: float   # harga awal
    mu: float              # drift (tren)
    sigma: float           # volatilitas
    dt: float = 0.0001     # time step

# Konfigurasi realistis per symbol
SYMBOL_CONFIGS: Dict[str, GBMConfig] = {
    "BTC-USD": GBMConfig(initial_price=65000.0, mu=0.0001, sigma=0.020),
    "ETH-USD": GBMConfig(initial_price=3200.0,  mu=0.0001, sigma=0.025),
    "BNB-USD": GBMConfig(initial_price=580.0,   mu=0.0001, sigma=0.022),
    "SOL-USD": GBMConfig(initial_price=145.0,   mu=0.0001, sigma=0.030),
    "ADA-USD": GBMConfig(initial_price=0.45,    mu=0.0001, sigma=0.028),
}

class GBMSimulator:
    """Simulate harga crypto menggunakan Geometric Brownian Motion."""

    def __init__(self, config: GBMConfig):
        self.config = config
        self.current_price = config.initial_price
        self.rng = np.random.default_rng()  # reproducible random

    def next_price(self) -> float:
        """Generate harga berikutnya menggunakan formula GBM."""
        mu, sigma, dt = self.config.mu, self.config.sigma, self.config.dt
        Z = self.rng.standard_normal()
        # GBM formula
        factor = np.exp((mu - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * Z)
        self.current_price *= factor
        return round(self.current_price, 8)

    def next_tick(self) -> dict:
        """Generate full tick data (price + bid/ask/volume)."""
        price = self.next_price()
        spread = price * 0.0002  # 0.02% spread bid-ask
        volume = abs(self.rng.normal(0.5, 0.3))  # volume ~normal
        return {
            "price":  price,
            "bid":    round(price - spread/2, 8),
            "ask":    round(price + spread/2, 8),
            "volume": round(volume, 6),
        }