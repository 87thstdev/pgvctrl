from setuptools import find_packages, setup
from pathlib import Path

CURRENT_DIR = Path(__file__).parent


def get_long_description() -> str:
    readme_md = CURRENT_DIR / "README.md"
    with open(readme_md, encoding="utf8") as ld_file:
        return ld_file.read()


setup(
    name="pgvctrl",
    version="0.8.1",
    description="postgreSQL database version control tool",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
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
