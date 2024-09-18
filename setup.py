from setuptools import setup, find_packages

setup(
    name="stofware-client-sdk",  # Replace with your package name
    version="0.1.0",
    author="Stofloos",
    author_email="robin@stofloos.nl",
    description="A client SDK for interacting with the Stofware API",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/stofloos/stofware-python-client-sdk",  
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "requests>=2.0.0",  # Required dependencies
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
