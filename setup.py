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
	author = 'Pete Cacioppi',
	author_email= 'peter.cacioppi@gmail.com',
	maintainer_email = '12samn@gmail.com, Kelley.Shannon.patricia@gmail.com',
	url = 'https://github.com/ticdat/ticdat/',
	classifiers = [
		"Development Status :: 5 - Production/Stable",
		"Intended Audience :: Developers",
		"Intended Audience :: Science/Research",
		"Operating System :: OS Independent",
		"Programming Language :: Python",
		"Programming Language :: Python :: 3",
		"Topic :: Scientific/Engineering",
		"Topic :: Scientific/Engineering :: Mathematics"
	],
	platforms = 'any'
)
