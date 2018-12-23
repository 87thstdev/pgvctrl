from setuptools import find_packages, setup
from pathlib import Path

CURRENT_DIR = Path(__file__).parent


def long_description() -> str:
    readme_md = CURRENT_DIR / "read.md"
    with open(readme_md, encoding="utf8") as ld_file:
        return ld_file.read()


setup(
    name="pgvctrl",
    version="0.8.0",
    description="postgreSQL database version control tool",
    keywords="postgres version control sql migrate migration patch patches",
    author="Heath Sutton",
    author_email="87thstreetdevelopment@gmail.com",
    url="https://github.com/87thstdev/pgvctrl",
    license="MIT",
    packages=find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests", "test"]
    ),
    long_description=long_description(),
    long_description_content_type="text/markdown",
    install_requires=["argparse", "simplejson", "plumbum"],
    entry_points={"console_scripts": ["pgvctrl=dbversioning.dbvctrl:main"]},
)
