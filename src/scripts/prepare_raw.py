import shutil, os, glob

LOG_DIR = "E:/Study/Data Engineering/projects/customer360/log_content"
RAW_BASE = "E:/Study/Data Engineering/projects/customer360/customer360-bigdata-pipeline/data/raw"

files = glob.glob(f"{LOG_DIR}/*.json")
for f in files:
    fname = os.path.basename(f)
    date_str = fname.replace(".json", "")
    year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
    load_date = f"{year}-{month}-{day}"
    target_dir = f"{RAW_BASE}/_load_date={load_date}"
    os.makedirs(target_dir, exist_ok=True)
    shutil.copy2(f, f"{target_dir}/{fname}")
    print(f"Copied {fname} -> {target_dir}/")

print(f"Done. {len(files)} files.")