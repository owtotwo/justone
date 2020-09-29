import setuptools
from justone import __version__ as justone_version

with open('Readme.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()


print(f'setuptools.find_packages() is {setuptools.find_packages()}')

setuptools.setup(
    name='justone',
    version=justone_version,
    author='owtotwo',
    author_email='owtotwo@163.com',
    description='Duplicate files finder',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/owtotwo/justone',
    py_modules=['justone'],
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3 :: Only',
        'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
        'Operating System :: Microsoft :: Windows :: Windows 10',
    ],
    python_requires='>=3.7',
    entry_points={
        'console_scripts': [
            'justone = justone:main',
        ],
    },
)
