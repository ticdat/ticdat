import ticdat
from setuptools import setup, find_packages

with open('README.md') as f:
    long_description = f.read()

setup(
	name = 'ticdat',
	packages = find_packages(),
	version = ticdat.__version__,
	long_description = long_description,
	long_description_content_type='text/markdown',
	license = 'BSD 2-Clause',
	author = 'Opalytics Inc',
	author_email= 'peter.cacioppi@accenture.com',
	maintainer_email = 'sam.s.nelson@accenture.com',
	url = 'https://github.com/opalytics/opalytics-ticdat',
	classifiers = [
		"Development Status :: 5 - Production/Stable",
		"Intended Audience :: Developers",
		"Intended Audience :: Science/Research",
		"Operating System :: OS Independent",
		"Programming Language :: Python",
		"Programming Language :: Python :: 2",
		"Programming Language :: Python :: 2.7",
		"Programming Language :: Python :: 3",
		"Programming Language :: Python :: 3.4",
		"Programming Language :: Python :: 3.5",
		"Topic :: Scientific/Engineering",
		"Topic :: Scientific/Engineering :: Mathematics"
	],
	platforms = 'any'
)
