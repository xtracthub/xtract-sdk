import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="xtract-sdk",
    version="0.0.6b1",
    author="Tyler J. Skluzacek",
    author_email="skluzacek@uchicago.edu",
    description="A package used for downloading and processing files from multiple habitats.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/xtracthub/xtract-sdk",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
