try:
    import setuptools
except ImportError:
    from distribute_setup import use_setuptools
    use_setuptools()
from setuptools import setup, find_packages

import sys, os

libdir = "lib"

sys.path.insert(0, libdir)

import tsar as pkg

setup_options = {
    "name": pkg.__project__,
    "version": pkg.__version__,
    "description": pkg.__description__,
    "long_description": pkg.__doc__,
    "classifiers": pkg.__classifiers__,
    "keywords": pkg.__keywords__,
    "author": pkg.__author__,
    "author_email": pkg.__author_email__,
    "url": pkg.__url__,
    "packages": find_packages(libdir),
    "package_dir": {"": libdir},
    "include_package_data": True,
    "zip_safe": False,
    "install_requires": pkg.__requires__,
    "entry_points": {
        "console_scripts": [
            "tsar = tsar.tsar:run",
            "tsar-collect-dcache-pnfsmanager = tsar.collectors.dcache_pnfsmanager:dcache_pnfsmanager.run",
            "tsar-collect-dcache-transfers = tsar.collectors.dcache_transfers:dcache_transfers.run",
            "tsar-collect-redis = tsar.collectors.redis:redis.run",
            "tsar-collect-sar = tsar.collectors.sar:sar.run",
        ],
    },
    "test_suite": "tests",
}

# Enable 2to3 on 3.x.
if sys.version_info >= (3, 0):
    setup_options["use_2to3"] = True

setup(**setup_options)
