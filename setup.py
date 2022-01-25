import setuptools

# How to update PyPi
# 1. Change the version number below
# 2. Run `python setup.py bdist_wheel`
# 3. Run `twine upload --skip-existing dist/*`

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt") as f:
    install_requires = f.readlines()

setuptools.setup(
    name="xtract-sdk",
    version="0.0.7a11",
    author="Tyler J. Skluzacek",
    author_email="skluzacek@uchicago.edu",
    description="A package used for downloading and processing files from multiple habitats.",
    install_requires=install_requires,
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
