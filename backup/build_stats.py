import rasterio
import numpy as np

f = r"C:\Users\My Pc\Documents\river project aiq\Imagery_Output\Sentinel_Merged\Ajay_sentinel_merged.tif"

with rasterio.open(f, "r+") as dst:
    print(f"Total bands: {dst.count}")
    for i in range(1, dst.count + 1):
        # Read at 10% resolution — much faster
        data = dst.read(i, out_shape=(1, dst.height // 10, dst.width // 10))
        valid = data.flatten()
        valid = valid[~np.isnan(valid) & (valid != -9999) & (valid != 0)]

        if valid.size == 0:
            print(f"  Band {i}: ❌ no valid data")
            continue

        bmin, bmax = float(np.percentile(valid, 2)), float(np.percentile(valid, 98))
        dst.update_tags(i,
            STATISTICS_MINIMUM=float(valid.min()),
            STATISTICS_MAXIMUM=float(valid.max()),
            STATISTICS_MEAN=float(valid.mean()),
            STATISTICS_STDDEV=float(valid.std())
        )
        print(f"  Band {i}: min={valid.min():.1f}  max={valid.max():.1f}  valid={valid.size:,}")

print("\nDone! Reload layer in QGIS.")