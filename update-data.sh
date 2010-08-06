for i in /var/log/sa/sa[0-9]*; do sadf -p $i; done | awk '{print $3, $5, $6}' | sort > iowait.raw
python sar2json.py < iowait.raw > iowait.json
