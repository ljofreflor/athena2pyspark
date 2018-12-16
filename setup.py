import setuptools

with open("README.md", "r") as fh:
    long_description = "description"  # fh.read().decode('unicode-escape')

setuptools.setup(
    name="athena2pyspark",
    version="0.0.4",
    author="Leonardo Jofre",
    author_email="ljofre2146@gmail.com",
    description="consumir athena desde spark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    install_requires=["pyspark", "boto3", "configparser"]
)

# instalador
# python setup.py sdist bdist_wheel
# twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
