from setuptools import setup
from setuptools import find_packages
from backtesting import __version__

setup(
    name='backtesting',
    version=__version__,
    description='strategy backtester',
    author='anthony marriott',
    author_email='anthony.marriott@lmax.com',
    url='',
    packages=find_packages(exclude=['tests*', 'testing*', 'notebooks*']),
    install_requires=[
        'pandas',
        'pathlib',
        'requests',
        'numpy',
        'jupyterlab'
    ],
)