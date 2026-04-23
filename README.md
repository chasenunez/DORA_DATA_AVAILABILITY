# DORA_DATA_AVAILABILITY

# To monitor progress

python3 -c "import sqlite3; c=sqlite3.connect('DATA/das_cache.sqlite'); \
n=c.execute(\"SELECT COUNT(*) FROM http_cache WHERE tier='epmc' AND key LIKE 'search|%'\").fetchone()[0]; \
print(f'{n}/81545 ({n/81545*100:.1f}%)')"

# to check the process
ps -p 70100 -o pid,etime,stat,rss,pcpu
When Phase 1 finishes
Output CSV at DATA/das_results_pass1.csv. Then run:

# Phase 2: retry unresolved DOIs with Tier 3 adapters enabled
python3 scripts/das_collect.py --retry-unknowns DATA/das_results_pass1.csv --deep \
    --out DATA/das_results_pass2.csv

# Merge Phase 1 + Phase 2 into final result
python3 scripts/merge_results.py DATA/das_results_pass1.csv DATA/das_results_pass2.csv \
    --out DATA/das_results_final.csv
If Phase 1 dies or you kill it (kill 70100), just re-run the original command:

nohup python3 scripts/das_collect.py --all --out DATA/das_results_pass1.csv \
    > DATA/pass1_run.log 2>&1 &