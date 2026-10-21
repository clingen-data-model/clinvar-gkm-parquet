"""Errors the tool raises. Each one stops the build: the tool never skips or drops input silently."""


class ToolError(Exception):
    """Base class. The CLI prints the message and exits non-zero."""


class InputError(ToolError):
    """An input can't be read: missing file, HTTP failure, checksum mismatch, malformed index or manifest."""


class BundleFormatError(ToolError):
    """A bundle line doesn't match the assembler's layout (design plan §4.2), or a section or key is unknown
    or duplicated. The message names the file, line number, section and key where known."""


class SchemaVersionError(ToolError):
    """A bundle's schema_version is missing or not in the accepted list (design plan §4.4)."""


class ChainError(ToolError):
    """A delta chain is broken: a missing week, a wrong order, or a delta that doesn't follow its full."""


class SpecError(ToolError):
    """The table spec is malformed, or a built table or record doesn't match it (an unknown extension name,
    a non-pointer where a pointer is required, a DESCRIBE that differs from the spec)."""
