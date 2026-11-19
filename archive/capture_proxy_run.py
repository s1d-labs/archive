"""Retired 2026-11-19 (owner: proxies are borrowed through the hub only).

This one-off tool captured India-only hosts from the server through the 10 Mumbai proxies, read straight from
/etc/archive/proxies.txt. That bypassed the hub, so its proxy use was not recorded. India-only capture now runs as pool
projects with proxy="IN" (archive-deep-india and friends): the hub lends each worker one proxy and records the lease
(and, from 2026-11-19, its traffic). Start such work there instead.
"""
import sys


def main(argv=None):
    sys.exit("archive.capture_proxy_run is retired: India-only capture runs as pool projects with proxy=\"IN\" "
             "(the hub lends and records the proxies). See this module's docstring.")


if __name__ == "__main__":
    main()
