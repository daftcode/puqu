from setuptools import setup, find_packages

test_requires = [
    'nose',
]


setup(
    name="puqu",
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'psycopg2',
    ],
    extras_require={
        'testing': test_requires,
    },
    include_package_data=True,
    zip_safe=False,
)
