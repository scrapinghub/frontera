from . import data
from .manager import CrawlGraphManager as Manager
from .models import CrawlPage as Page
from .models import CrawlPageRelation as Relation

__all__ = [
    "Manager",
    "Page",
    "Relation",
    "data",
]
