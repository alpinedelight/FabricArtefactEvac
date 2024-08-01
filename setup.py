from setuptools import find_packages,setup

setup(
    name='fabricmassevac',
    version='0.1',
    package_dir={"":"app"},
    packages=find_packages(where="app"),
    python_requires=">=3.10"
)