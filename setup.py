import sys
from codecs import open  # To use a consistent encoding
from os import path

# Always prefer setuptools over distutils
from setuptools import (setup, find_packages)

here = path.abspath(path.dirname(__file__))
install_requirements = [
    'aiodns~=1.1.1',
    'PyYAML~=3.12',
]

# The following are meant to avoid accidental upload/registration of this
# package in the Python Package Index (PyPi)
pypi_operations = frozenset(['register', 'upload']) & frozenset([x.lower() for x in sys.argv])
if pypi_operations:
    raise ValueError('Command(s) {} disabled in this example.'.format(', '.join(pypi_operations)))

with open(path.join(here, 'README.rst'), encoding='utf-8') as fh:
    long_description = fh.read()

__version__ = None
exec(open('pfstatsd/about.py').read())
if __version__ is None:
    raise IOError('about.py in project lacks __version__!')

setup(name='pfstatsd', version=__version__,
      author='Ben Jolitz',
      description='measure my pf stats',
      long_description=long_description,
      license='BSD',
      packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
      include_package_data=True,
      extras_require={
          'tests': ['pytest~=3.4.1', 'pytest-asyncio~=0.8.0'],
          'fast': ['uvloop~=0.9.1'],
      },
      install_requires=install_requirements,
      keywords=['pf', 'graphite'],
      url="https://github.com/benjolitz/pfstatsd",
      classifiers=[
          "Development Status :: 3 - Alpha",
          "Topic :: Utilities",
          "License :: OSI Approved :: BSD License",
      ])
