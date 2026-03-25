import yaml
import json
import os

def read_yaml(path):
    """Internal helper to read a single YAML file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found at: {path}")
    
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}

def read_json(path):
    """Internal helper to read a single JSON file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found at: {path}")
    
    with open(path, "r") as f:
        return json.load(f) or {}
