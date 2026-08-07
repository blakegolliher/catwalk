import sys
from pathlib import Path

# Make `import mock_catalog` work regardless of how pytest is invoked.
sys.path.insert(0, str(Path(__file__).parent))
