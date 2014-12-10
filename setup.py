from setuptools import setup, find_packages

import versioneer
versioneer.VCS = 'git'
versioneer.versionfile_source = 'crawlfrontier/_version.py'
versioneer.versionfile_build = 'crawlfrontier/_version.py'
versioneer.tag_prefix = 'v'  # tags are like v1.2.0
versioneer.parentdir_prefix = 'crawlfrontier-'


setup(
    name='crawl-frontier',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=find_packages(exclude=('tests', 'tests.*', 'examples', 'examples.*')),
    url='https://github.com/scrapinghub/crawl-frontier',
    description='A flexible frontier for web crawlers',
    author='Scrapy developers',
    maintainer='Javier Casas',
    maintainer_email='javier@scrapinghub.com',
    license='BSD',
    include_package_data=True,
    zip_safe=False,
    keywords=['crawler', 'frontier', 'scrapy', 'web', 'requests'],
    classifiers=[
        #'Framework :: Crawl Frontier',
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
        'w3lib>=1.10.0',
        'tldextract>=1.5.1',
    ],
    extras_require={
        'graphs': [
            "pyparsing==1.5.7",
            "pydot==1.0.28",
            "SQLAlchemy>=0.9.8",
        ],
        'logging': [
            "colorlog>=2.4.0",
        ],
    },
    tests_require=[
        "pytest>=2.6.4",
        "SQLAlchemy>=0.9.8",
    ]
)
