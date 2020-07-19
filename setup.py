from setuptools import setup, find_packages

setup(
   name='wsclient',
   version='0.1.0',
   description='A framework for implementing websocket APIs',
   author='binares',
   packages=find_packages(exclude=['test']),
   python_requires='>=3.5',
   install_requires=[
       'requests>=2.18.4',
       'websockets>=4.0.1',
       'python-socketio[asyncio_client]>=4.6.0', # tested 4.6
       'fons @ git+https://github.com/binares/fons.git',
   ],
)
