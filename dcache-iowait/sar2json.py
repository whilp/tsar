import json
import sys

data = {}
splitted = (l.strip().split(None, 2) for l in sys.stdin)
converted = ((k, int(x), float(y)) for x, k, y in splitted)
for k, x, y in converted:
    if k in "%idle %nice %steal".split(): continue
    data.setdefault(k, []).append([x, y])

json.dump({"data": data}, sys.stdout)
