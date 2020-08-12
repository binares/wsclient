import pathlib
from setuptools import setup, find_packages

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

setup(
   name='wsclient',
   version='0.2.0',
   description='A framework for implementing websocket APIs',
   long_description=README,
   long_description_content_type='text/markdown',
   url='https://github.com/binares/fons',
   author='binares',
   author_email='binares@protonmail.com',
   license='MIT',
   packages=find_packages(exclude=['test']),
   python_requires='>=3.5',
   install_requires=[
       'requests>=2.18.4',
       'websockets>=4.0.1',
       'python-socketio[asyncio_client]>=4.6.0', # tested 4.6
       'fons>=0.2.0',
   ],
)
