import setuptools
import os

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
     name='emulator-tools',  
     version='0.1.1',
     scripts=['emulator-tools'] ,
     author="AYLIEN Engineering",
     author_email="eng-team@aylien.com",
     description="Set of tools for interacting with Google Cloud Emulators",
     long_description=long_description,
     long_description_content_type="text/markdown",
     url="https://github.com/aylien/emulator-tools",
     packages=['tools'],
     install_requires=[
        "google-cloud-pubsub==1.1.0",
        "google-cloud-bigtable==1.2.1",
        "google-cloud-core==1.1.0"
     ],
     classifiers=[
         "Programming Language :: Python :: 3",
         "License :: OSI Approved :: MIT License",
         "Operating System :: OS Independent",
     ],
 )
