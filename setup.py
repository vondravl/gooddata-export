"""Setup configuration for gooddata-export."""

from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="gooddata-export",
    version="1.0.0",
    author="Vlastimil Vondra",
    description="Export GoodData workspace metadata to SQLite and CSV",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    python_requires=">=3.11",
    install_requires=[
        "python-dotenv>=1.0.0",
        "requests>=2.32.0",
        "pyyaml>=6.0.0",
        "ruff>=0.8.0",
    ],
    package_data={
        "gooddata_export": [
            "sql/*.sql",
            "sql/*.yaml",
            "sql/views/*.sql",
            "sql/updates/*.sql",
        ],
    },
    include_package_data=True,
)
