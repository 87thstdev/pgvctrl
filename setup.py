from setuptools import find_packages, setup
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "readme.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="pgvctrl",
    version="0.7.1",
    description="postgreSQL database version control tool",
    keywords="postgres version control sql migrate migration patch patches",
    author="Heath Sutton",
    author_email="87thstreetdevelopment@gmail.com",
    url="https://github.com/87thstdev/pgvctrl",
    license="MIT",
    packages=find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests", "test"]
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=["argparse", "simplejson", "plumbum"],
    entry_points={"console_scripts": ["pgvctrl=dbversioning.dbvctrl:main"]},
)
