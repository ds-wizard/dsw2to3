from setuptools import setup, find_packages


with open('README.md') as f:
    long_description = ''.join(f.readlines())

setup(
    name='dsw2to3',
    version='1.0.1',
    description='CLI tool to support migration from DSW 2.14 to DSW 3.0',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Marek Suchánek',
    keywords='dsw migration database upgrade',
    license='Apache License 2.0',
    url='https://github.com/ds-wizard/dsw2to3',
    packages=find_packages(),
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Utilities',
    ],
    zip_safe=False,
    python_requires='>=3.6, <4',
    install_requires=[
        'click',
        'minio',
        'pymongo',
        'PyYAML',
        'psycopg2',
        'tenacity',
    ],
    setup_requires=[
        'wheel',
    ],
    entry_points={
        'console_scripts': [
            'dsw2to3=dsw2to3:main',
        ],
    },
)
