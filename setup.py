from setuptools import setup, find_packages

setup(
   name='wsclient',
   version='0.1.0',
   description='A framework for implementing websocket APIs',
   author='binares',
   packages=find_packages(exclude=['test']),
   python_requires='>=3.5',
   install_requires=[
       'websockets>=3.0',
       'signalr_client_aio>=0.0.1.6.2',
       'fons @ git+https://github.com/binares/fons.git',
   ],
)
