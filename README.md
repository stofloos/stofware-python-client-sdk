# Stofware Client

Stofware Client is a Python SDK for interacting with the Stofware API.

## Installation

You can install the package using pip:

```bash
pip install stofware-client
```

Usage
```python
from stofware_client import StofwareClient

client = StofwareClient("https://api.example.com", "your-token-here")
data = client.model("your-entity").get_all()
