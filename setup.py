from setuptools import setup, find_packages

setup(
    name='src',
    version='0.0.1',
    author_email='dvbuchko@gmail.com',
    packages=find_packages(),
    include_package_data=True,
    py_modules=['src'],
    install_requires=[i.strip() for i in open('requirements.txt')],
    entry_points={
        'console_scripts': [
            'data_downloader = scripts.data_downloader:main',
        ],
    },
)