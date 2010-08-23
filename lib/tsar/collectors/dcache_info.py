from functools import partial
from itertools import chain

from . import helpers
from .commands import Collector

class DcacheInfo(Collector):
    
    def main(self):
        import lxml.etree

        t = self.now
        doc = lxml.etree.parse(self.params.url[0])
        root = doc.getroot()

        subject = "dcache"
        data = [(subject, k.replace('-', '').lower(), t, v) \
            for k, v in self.parse(root).items()]
        self.submit(data)

    def parse(self, root):
        # Helpers.
        nsmap = root.nsmap.copy()
        nsmap["i"] = nsmap.pop(None)
        expath = lambda e, p, **k: e.xpath(p, namespaces=nsmap, **k)
        xpath = partial(expath, root)

        pctkeys ="free precious used".split()

        data = {}

        # Domains and cells.
        data["domains_all_count"] = len(xpath("i:domains/*"))
        data["cells_all_count"] = len(set(xpath("i:domains/i:domain/i:cells/i:cell/@name")))

        # Doors.
        for door in xpath("i:doors/i:door"):
            protos = expath(door, "i:protocol/i:metric[@name='family']/text()")
            load = float(expath(door, "i:metric[@name='load']/text()")[0])
            for proto in chain(["all"], protos):
                helpers.appendkey(data, "doors_%s_load" % proto, load)
                helpers.incrkey(data, "doors_%s_count" % proto)

        # Space summary.
        data.update(("space_%s_bytes" % m.get("name"), int(m.text)) for m in \
            xpath("i:summary/i:pools/i:space/i:metric"))
        total = data["space_total_bytes"]
        for k in pctkeys:
            base = "space_%s" % k
            data["%s_pct" % base] = helpers.pct(data["%s_bytes" % base], total)

        # Pools.
        pgroups = ["all"]
        for pool in xpath("i:pools/i:pool"):
            helpers.incrkey(data, "pools_count")
            pspace = dict((m.get("name"), helpers.intorfloat(m.text)) \
                for m in expath(pool, "i:space/i:metric"))
            if not pspace:
                helpers.incrkey(data, "pools_missing_count")
                continue

            # XXX: Heartbeat?

            groups = expath(pool, "i:poolgroups/i:poolgroupref/@name")
            if "false" in expath(pool, "i:metric[@name='enabled']/text()"):
                groups.append("disabled")
            if "true" in expath(pool, "i:metric[@name='read-only']/text()"):
                groups.append("readonly")
            pgroups.extend(groups)

            skip = "LRU-seconds break-even".split()
            for group in chain(["all"], groups):
                for k, v in pspace.items():
                    if k in skip: continue
                    helpers.appendkey(data, "poolgroups_%s_%s_bytes" % (group, k), v)
                    helpers.incrkey(data, "poolgroups_%s_%s_sum_bytes" % (group, k), v)
                    helpers.incrkey(data, "poolgroups_all_%s_sum_bytes" % k, v)

                for k in pctkeys:
                    helpers.appendkey(data, "poolgroups_%s_%s_pct" % (group, k),
                        helpers.pct(pspace[k], pspace["total"]))

            for queue in expath(pool, "i:queues//i:queue"):
                name = queue.get("name", queue.get("type"))
                name = helpers.rtrim(name, "queue")
                for metric in expath(queue, "i:metric"):
                    value, metric = int(metric.text), metric.get("name")
                    helpers.appendkey(data, "queues_all_%s_count" % metric, value)
                    helpers.incrkey(data, "queues_all_%s_sum_count" % metric, value)
                    helpers.appendkey(data, "queues_%s_%s_count" % (name, metric), value)

        return data

    def setup(self):
        Collector.setup(self)
        self.argparser = self.parent.subparsers.add_parser("dcache-info", 
            help="dCache info cell")
        self.add_param("url", nargs=1, help="info cell XML URL")
