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
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    install_requires=[
        'six>=1.8.0',
        'w3lib>=1.10.0'
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
        ],
        'tldextract': [
            'tldextract>=1.5.1',
        ],
        'hbase': [
            'happybase'
        ],
        'zeromq': [
            'pyzmq',
            'msgpack-python'
        ],
        'kafka': [
            'kafka-python<=0.9.5'
        ],
        'distributed': [
            'Twisted'
        ]
    },
    tests_require=[
        "pytest>=2.6.4",
        "MySQL-python>=1.2.5",
        "PyMySQL>=0.6.3",
        "psycopg2>=2.5.4",
        "scrapy>=0.24",
        "tldextract>=1.5.1",
        "SQLAlchemy>=1.0.0",
        "cachetools"
    ]
)
