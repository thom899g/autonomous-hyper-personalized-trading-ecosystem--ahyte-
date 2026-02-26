"""
AHYTE Configuration and Firebase Initialization.
Centralized configuration management and Firebase Admin SDK setup.
"""
import os
import logging
from typing import Dict, Any, Optional
import json
from pathlib import Path

# Firebase Admin is CRITICAL for ecosystem state management
import firebase_admin
from firebase_admin import credentials, firestore, auth
import google.cloud.firestore

# Type hints for configuration
ConfigDict = Dict[str, Any]

class AHYTEConfig:
    """Centralized configuration management with validation."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        self._config: ConfigDict = {}
        self.firestore_client: Optional[google.cloud.firestore.Client] = None
        
        # Default configuration
        self._defaults = {
            "trading": {
                "max_position_size": 0.1,  # 10% of portfolio per trade
                "default_timeframe": "1h",
                "supported_exchanges": ["binance", "coinbase", "kraken"]
            },
            "risk": {
                "max_drawdown": 0.25,  # 25% maximum drawdown
                "volatility_threshold": 0.02,
                "minimum_win_rate": 0.45
            },
            "learning": {
                "retrain_interval_hours": 24,
                "minimum_samples": 100,
                "validation_split": 0.2
            },
            "firebase": {
                "collection_traders": "traders",
                "collection_strategies": "strategies",
                "collection_trades": "trades",
                "collection_market_data": "market_data"
            }
        }
        
        self.load_config(config_path)
        self.initialize_firebase()
    
    def load_config(self, config_path: Optional[str]) -> None:
        """Load configuration from file or environment variables."""
        try:
            if config_path and Path(config_path).exists():
                with open(config_path, 'r') as f:
                    file_config = json.load(f)
                self._config = {**self._defaults, **file_config}
                self.logger.info(f"Loaded config from {config_path}")
            else:
                self._config = self._defaults
                self.logger.info("Using default configuration")
            
            # Override with environment variables if present
            self._override_from_env()
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in config file: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to load config: {e}")
            raise
    
    def _override_from_env(self) -> None:
        """Override config with environment variables."""
        env_mappings = {
            "AHYTE_MAX_POSITION_SIZE": ("trading", "max_position_size", float),
            "AHYTE_MAX_DRAWDOWN": ("risk", "max_drawdown", float),
            "FIREBASE_PROJECT_ID": ("firebase", "project_id", str),
        }
        
        for env_var, (section, key, type_cast) in env_mappings.items():
            if env_var in os.environ:
                try:
                    self._config[section][key] = type_cast(os.environ[env_var])
                    self.logger.debug(f"Overridden {section}.{key} from env")
                except (ValueError, KeyError) as e:
                    self.logger.warning(f"Failed to cast env var {env_var}: {e}")
    
    def initialize_firebase(self) -> None:
        """Initialize Firebase Admin SDK with error handling."""
        try:
            # Check if Firebase is already initialized
            if not firebase_admin._apps:
                # Priority 1: Environment variable with service account JSON
                if "FIREBASE_SERVICE_ACCOUNT" in os.environ:
                    cred_dict = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT"])
                    cred = credentials.Certificate(cred_dict)
                # Priority 2: Service account file
                elif Path("serviceAccountKey.json").exists():
                    cred = credentials.Certificate("serviceAccountKey.json")
                # Priority 3: Default application credentials (GCP)
                else:
                    cred = credentials.ApplicationDefault()
                    self.logger.info("Using GCP default credentials")
                
                firebase_admin.initialize_app(cred)
                self.logger.info("Firebase Admin SDK initialized successfully")
            
            self.firestore_client = firestore.client()
            # Test connection
            test_doc = self.firestore_client.collection("health").document("test")
            test_doc.set({"timestamp": firestore.SERVER_TIMESTAMP})
            test_doc.delete()
            self.logger.debug("Firestore connection test successful")
            
        except FileNotFoundError as e:
            self.logger.error(f"Firebase service account file not found: {e}")
            raise
        except ValueError as e:
            self.logger.error(f"Invalid Firebase credentials: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to initialize Firebase: {e}")
            raise
    
    def get(self, *keys: str, default: Any = None) -> Any:
        """Safely retrieve nested configuration values."""
        current = self._config
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current
    
    @property
    def firebase_collections(self) -> Dict[str, str]:
        """Get Firebase collection names."""
        return self._config.get("firebase", {})

# Global configuration instance
config = AHYTEConfig()