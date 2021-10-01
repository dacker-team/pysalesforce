import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pysalesforce",
    version="0.0.9",
    author="Dacker",
    author_email="hello@dacker.co",
    description="A Salesforce connector",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dacker-team/pysalesforce",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3',
    install_requires=[
        "dbstream>=0.0.16",
        "pyyaml>=5.3.1"
    ],
)
