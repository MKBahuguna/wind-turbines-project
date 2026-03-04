import yaml
import logging
from typing import Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class ConfigLoader:
    """Load and parse YAML configuration files"""
    
    @staticmethod
    def load_yaml(config_path: str) -> Dict[str, Any]:
        """
        Load YAML config file
        
        Args:
            config_path: Path to YAML file (relative or absolute)
            
        Returns:
            Dictionary with config values
        """
        # Convert to Path object for easier handling
        path = Path(config_path)
        
        # If relative path, look in project config directory
        if not path.is_absolute():
            config_dir = Path(__file__).parent.parent.parent / "config"
            path = config_dir / config_path
        
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        
        logger.info(f"Loading config from: {path}")
        
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
        
        if not config:
            raise ValueError(f"Config file is empty: {path}")
        
        logger.info(f"Config loaded successfully")
        return config
    
    @classmethod
    def load_config(cls, layer: str) -> Dict[str, Any]:
        """
        Load config for specified layer
        
        Args:
            layer: Layer name (bronze, silver, gold, or any custom layer)
            
        Returns:
            Dictionary with layer configuration
        """
        return cls.load_yaml(f"{layer}_config.yaml")
