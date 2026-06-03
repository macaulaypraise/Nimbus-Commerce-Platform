"""In-process fakes for external services used in tests.

Each fake in this package implements a minimal subset of the real
client API sufficient for the unit tests. They live in their own
package (not in conftest.py) so they can be imported by type
checkers and reused across test modules.
"""
