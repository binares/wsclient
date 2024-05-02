import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

with open(HERE / "requirements.txt", encoding="utf-8") as f:
    requirements = f.read().split("\n")

setup(
    name="wsclient",
    version="0.4.0",
    description="A framework for implementing websocket APIs",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/binares/wsclient",
    author="binares",
    author_email="binares@protonmail.com",
    license="MIT",
    packages=find_packages(exclude=["test"]),
    python_requires=">=3.5",
    install_requires=requirements,
)
