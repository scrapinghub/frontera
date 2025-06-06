from setuptools import find_packages, setup

setup(
    name="frontera",
    version="0.7.2",
    packages=find_packages(exclude=("tests", "tests.*", "examples", "examples.*")),
    url="https://github.com/scrapinghub/frontera",
    description="A scalable frontier for web crawlers",
    author="Frontera developers",
    maintainer="Alexander Sibiryakov",
    maintainer_email="sibiryakov@scrapinghub.com",
    license="BSD",
    include_package_data=True,
    zip_safe=False,
    keywords=["crawler", "frontier", "scrapy", "web", "requests", "frontera"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    install_requires=["w3lib>=1.17.0"],
    python_requires=">=3.9",
    extras_require={
        "s3": [
            "boto>=2.49.0",
        ],
        "scrapy": [
            "scrapy>=2.7.0",
        ],
        "sql": ["cachetools>=0.4.0", "SQLAlchemy>=1.0.0,<1.4"],
        "graphs": ["pyparsing==1.5.7", "pydot==1.0.28", "SQLAlchemy"],
        "logging": ["colorlog>=2.4.0", "python-json-logger>=0.1.5"],
        "tldextract": [
            "tldextract>=1.5.1",
        ],
        "hbase": [
            "cachetools>=0.4.0",
            "happybase>=1.2.0",
            "msgpack-python>=0.4",
            # https://github.com/python-happybase/happybase/pull/261
            "setuptools>=50.3.1",
        ],
        "zeromq": ["pyzmq>=19.0.2", "msgpack-python>=0.4"],
        "kafka": ["kafka-python>=1.4.3,<2.1", "twisted>=20.3.0"],
        "distributed": ["Twisted"],
    },
)
