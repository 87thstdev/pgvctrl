from setuptools import find_packages, setup


def readme():
    with open('readme.md') as f:
        return f.read()


setup(
    name='pgvctrl',
    version='0.5.0',
    description='postgreSQL database version control tool',
    long_description=readme(),
    keywords='postgres version control sql migrate migration patch patches',
    author='Heath Sutton',
    author_email='87thstreetdevelopment@gmail.com',
    url='https://github.com/87thstdev/pgvctrl',
    license='MIT',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests", "test"]),
    install_requires=[
        'argparse',
        'simplejson',
        'plumbum',
    ],
    entry_points={
        'console_scripts': ['pgvctrl=dbversioning.dbvctrl:main'],
    },
)
