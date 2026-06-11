from unittest.mock import MagicMock

import eodhp_utils.runner

# Prevent the module-level create_producer call in app.py from connecting to Pulsar
# when test modules import the app.
eodhp_utils.runner.pulsar_client = MagicMock()
