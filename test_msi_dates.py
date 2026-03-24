import sys, os
sys.path.append(os.getcwd())
from fetch_msi import fetch_msi_single, PRIMARY_MSI_URL_TEMPLATE

res = fetch_msi_single('4', PRIMARY_MSI_URL_TEMPLATE, 'primary')
print(f"Got {len(res)} warnings from Area 4.")
for i, s in enumerate(res[:10]):
    print(f"\n--- Warning {i} ---")
    print(s.get('msgText'))
