import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="athena2pyspark",
    version="0.0.2",
    author="Leonardo Jofre",
    author_email="ljofre2146@gmail.com",
    description="consumir athena desde spark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages()
)
