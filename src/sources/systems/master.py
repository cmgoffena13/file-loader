from src.sources.registry import SourceRegistry
from src.sources.systems.customer.customer import CUSTOMERS
from src.sources.systems.financial.financial import FINANCIAL
from src.sources.systems.inventory.inventory import INVENTORY
from src.sources.systems.sales.sales import SALES

MASTER_REGISTRY = SourceRegistry()
MASTER_REGISTRY.add_sources([INVENTORY, SALES, FINANCIAL, CUSTOMERS])
