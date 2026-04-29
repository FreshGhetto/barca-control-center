import sys
sys.path.insert(0, '.')
from pathlib import Path
from barca_control_center.ingest_agent import _read_preview, _classify_file, _extract_season_code, _extract_report_date

folder = Path(r'C:\Users\ufficio2\Desktop\stampe da aggiungere')
for f in sorted(folder.iterdir()):
    if f.suffix.lower() not in {'.csv', '.xlsx', '.xls', '.xlsm'}:
        continue
    preview = _read_preview(f)
    cls = _classify_file(f, preview)
    season = _extract_season_code(preview, f.name)
    rdate = _extract_report_date(preview)
    kind = cls['kind']
    conf = cls['confidence']
    reasons = cls.get('reasons', [])
    print(f"{f.name:30s}  kind={kind:25s}  conf={conf:.2f}  season={season}  date={rdate}")
    print(f"  reasons: {reasons}")
    print()

