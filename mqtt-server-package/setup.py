from setuptools import setup, find_packages

setup(
    name="mqtt_server",
    version="0.1.0",
    packages=find_packages(),
    python_requires = ">=3.10",
    include_package_data=True, 
    install_requires=[
        "bcrypt",
        "bitstruct"
    ],
)