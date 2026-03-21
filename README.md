# notam-whisper
Auto-updated NOTAM KML viewer

## MSI data source fallback

`fetch_msi.py` supports an optional fallback MSI source when `msi.nga.mil` is unstable.

Set environment variable `MSI_FALLBACK_URL_TEMPLATE` to an XML endpoint template that contains `{nav_area}`.

Example:

`MSI_FALLBACK_URL_TEMPLATE="https://example.com/smaps?navArea={nav_area}&status=active&output=xml" python fetch_msi.py`
