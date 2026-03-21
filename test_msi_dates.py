import sys, os
sys.path.append(os.getcwd())
from fetch_msi import fetch_msi_single

res = fetch_msi_single('4')
print(f"Got {len(res)} warnings from Area 4.")
for i, s in enumerate(res[:10]):
    print(f"\n--- Warning {i} ---")
    print(s.get('msgText'))
