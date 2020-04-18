import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dbops",
    version="0.0.1",
    author="Stuart Ianna",
    author_email="stuian@protonmail.com",
    description="Python class helper for working with sqlite3 databases and Pandas dataframes.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/stuianna/DBOps",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'pandas',
        'numpy'
    ],
    py_modules=['dbops'],
    python_requires='>=3.6',
)
