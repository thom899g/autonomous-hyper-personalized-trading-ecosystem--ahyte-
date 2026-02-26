"""
Real-time Market Data Pipeline with Edge Case Handling.
Collects, validates, and stores market data from multiple exchanges.
"""
import asyncio
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import time

import pandas as pd
import numpy as np
import ccxt  # Standard cryptocurrency exchange library
from google.cloud import firestore

from config import config

class MarketDataPipeline:
    """Robust market data collection with validation and error recovery."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.firestore = config.firestore_client
        self.exchanges: Dict[str, ccxt.Exchange] = {}
        self._initialize_exchanges()
        
        # Rate limiting and circuit breaker state
        self._rate_limits: Dict[str, Dict] = {}
        self._circuit_breakers: Dict[str, bool] = {}
        self._last_fetch: Dict[str, datetime] = {}
        
    def _initialize_exchanges(self) -> None:
        """Initialize exchange connections with error handling."""
        supported = config.get("trading", "supported_exchanges", default=[])
        
        for exchange_id in supported:
            try:
                exchange_class = getattr(ccxt, exchange_id)
                exchange = exchange_class({
                    'enableRateLimit': True,
                    'timeout': 30000,
                    'rateLimit': 1200,
                })
                
                # Test connection with a lightweight call
                exchange.load_markets()
                self.exchanges[exchange_id] = exchange
                self._circuit_breakers[exchange_id] = False
                self.logger.info(f"Initialized {exchange_id} exchange")
                
            except AttributeError:
                self.logger.warning(f"Exchange {exchange_id} not