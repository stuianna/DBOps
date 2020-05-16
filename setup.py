import setuptools
exec(open('dbops/_version.py').read())

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="db-ops",
    version=__version__,
    author="Stuart Ianna",
    author_email="stuian@protonmail.com",
    description="",
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
        'numpy',
        'pyrfc3339',
        'influxdb'
    ],
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
)
