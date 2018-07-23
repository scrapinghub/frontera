from setuptools import setup, find_packages

import versioneer
versioneer.VCS = 'git'
versioneer.versionfile_source = 'frontera/_version.py'
versioneer.versionfile_build = 'frontera/_version.py'
versioneer.tag_prefix = 'v'  # tags are like v1.2.0
versioneer.parentdir_prefix = 'frontera-'


setup(
    name='frontera',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=find_packages(exclude=('tests', 'tests.*', 'examples', 'examples.*')),
    url='https://github.com/scrapinghub/frontera',
    description='A scalable frontier for web crawlers',
    author='Frontera developers',
    maintainer='Alexander Sibiryakov',
    maintainer_email='sibiryakov@scrapinghub.com',
    license='BSD',
    include_package_data=True,
    zip_safe=False,
    keywords=['crawler', 'frontier', 'scrapy', 'web', 'requests', 'frontera'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    install_requires=[
        'six>=1.8.0',
        'w3lib>=1.15.0',
        'cityhash>=0.1.7'
    ],
    extras_require={
        'sql': [
            'SQLAlchemy>=1.0.0',
            'cachetools'
        ],
        'graphs': [
            'pyparsing==1.5.7',
            'pydot==1.0.28',
            'SQLAlchemy'
        ],
        'logging': [
            'colorlog>=2.4.0',
            'python-json-logger>=0.1.5'
        ],
        'tldextract': [
            'tldextract>=1.5.1',
        ],
        'hbase': [
            'happybase>=1.0.0'
        ],
        'zeromq': [
            'pyzmq',
            'msgpack-python>=0.4'
        ],
        'kafka': [
            'kafka-python>=1.4.0'
        ],
        'distributed': [
            'Twisted'
        ],
        's3': [
            'boto3'
        ],
        'redis': [
            'redis>=2.10.5',
            'hiredis>=0.2'
        ],
        'strategies': [
            'beautifulsoup4',
            'publicsuffix'
        ]
    },
    tests_require=[
        "pytest>=2.6.4",
        "PyMySQL>=0.6.3",
        "psycopg2>=2.5.4",
        "scrapy>=0.24",
        "tldextract>=1.5.1",
        "SQLAlchemy>=1.0.0",
        "cachetools",
        "mock",
        "boto>=2.42.0",
        "colorlog>=2.4.0",
        "python-json-logger>=0.1.5",
        "redis>=2.10.5",
        "hiredis>=0.2"
    ]
)
