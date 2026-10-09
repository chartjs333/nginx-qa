"""Versioned nginx-qa services introduced outside the legacy monolith.

Modules in this package must remain import-side-effect free.  In particular,
importing a contract module must never read or write nginx-qa runtime state.
"""
