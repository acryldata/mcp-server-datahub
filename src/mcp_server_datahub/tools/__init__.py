"""MCP tools for DataHub integrations."""

from .dataset_queries import get_dataset_queries
from .descriptions import update_description
from .documents import grep_documents, search_documents
from .domains import remove_domains, set_domains
from .entities import get_entities, list_schema_fields
from .get_me import get_me
from .lineage import get_lineage, get_lineage_paths_between
from .owners import add_owners, remove_owners
from .search import enhanced_search, search
from .structured_properties import (
    add_structured_properties,
    remove_structured_properties,
)
from .tags import add_tags, remove_tags
from .terms import (
    add_glossary_terms,
    remove_glossary_terms,
)

__all__ = [
    "add_glossary_terms",
    "add_owners",
    "add_structured_properties",
    "add_tags",
    "enhanced_search",
    "get_dataset_queries",
    "get_entities",
    "get_lineage",
    "get_lineage_paths_between",
    "get_me",
    "grep_documents",
    "list_schema_fields",
    "remove_domains",
    "remove_glossary_terms",
    "remove_owners",
    "remove_structured_properties",
    "remove_tags",
    "search",
    "search_documents",
    "set_domains",
    "update_description",
]
