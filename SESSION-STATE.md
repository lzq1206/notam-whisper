# Session State

## WAL

- 2026-09-14: Add Guiana Space Centre / ELA-1 launch fallback for Sentinel-3C / Vega VV30. Extend launch-context FIR coverage to SOOO, TTZP, and KZWY so local and downrange aerospace NOTAMs are searched even when the remote launch feed omits the mission. Preserve the existing MSI and Baikonur changes.
- 2026-09-14: Add Vandenberg SFB / SLC-4E launch fallback for USSF-259. The remote launch feed omits the classified mission, while KZLA and KZAK are already in the supplemental FIR list; add local schedule context and explicit Vandenberg aliases so launch-correlated searches continue to cover the local and Pacific downrange regions. No current NOTAM was manually added because live KZLA rocket/airspace results did not identify a USSF-259 notice.
