from setuptools import find_packages, setup


setup(
    name='pgvctrl',
    version='0.2.0',
    description='postgreSQL database version control tool',
    author='Heath Sutton',
    author_email='87thstreetdevelopment@gmail.com',
    license='MIT',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    install_requires=[
        'argparse',
        'simplejson',
        'plumbum',
    ],
    entry_points={
        'console_scripts': ['pgvctrl=dbversioning.dbvctrl:main'],
    },
)