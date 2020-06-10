import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

# TODO: Version like https://github.com/xtracthub/xtract-container-service/blob/master/setup.py
setuptools.setup(
    name="xtract-sdk", # Replace with your own username
    version="0.0.1",
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
