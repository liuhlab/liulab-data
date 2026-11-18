# ruff: noqa
import sys, time, json, urllib.request, urllib.parse

B = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"


def get(util, **p):
    p.setdefault("tool", "labdata-research")
    url = B + util + ".fcgi?" + urllib.parse.urlencode(p)
    err = None
    for i in range(4):
        try:
            time.sleep(0.4)
            return urllib.request.urlopen(url, timeout=60).read()
        except Exception as e:
            time.sleep(2 + i * 3)
            err = e
    raise err


def fetch(url):
    err = None
    for i in range(4):
        try:
            time.sleep(0.4)
            return urllib.request.urlopen(url, timeout=60).read()
        except Exception as e:
            if getattr(e, "code", None) == 404:
                return None
            time.sleep(2 + i * 3)
            err = e
    raise err


def search(term, retmax=0, **kw):
    r = json.loads(get("esearch", db="gds", term=term, retmax=retmax, retmode="json", **kw))[
        "esearchresult"
    ]
    return int(r["count"]), r.get("idlist", []), r.get("querytranslation")


if __name__ == "__main__":
    for t in sys.argv[1:]:
        c, _, q = search(t)
        print(c, "|", t, "|", q)
