from setuptools import find_packages, setup
from pathlib import Path

CURRENT_DIR = Path(__file__).parent


def readme():
    with open('readme.rst', encoding="utf8") as f:
        return f.read()


setup(
    name="pgvctrl",
    version="1.0.0",
    description="postgreSQL database version control tool",
    long_description=readme(),
    long_description_content_type="text/x-rst",
    python_requires=">=3.6",
    keywords="postgres version control sql migrate migration patch patches",
    author="Heath Sutton",
    author_email="87thstreetdevelopment@gmail.com",
    url="https://github.com/87thstdev/pgvctrl",
    license="MIT",
    packages=find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests", "test"]
    ),
    install_requires=["argparse", "simplejson", "plumbum"],
    entry_points={"console_scripts": ["pgvctrl=dbversioning.dbvctrl:main"]},
)
