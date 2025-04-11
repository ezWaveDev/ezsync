from setuptools import setup, find_packages

setup(
    name="ezSync",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "requests>=2.31.0",
        "python-dotenv>=1.0.0",
        "geopy>=2.4.1",
        "pyodbc>=5.0.1",
    ],
    entry_points={
        'console_scripts': [
            'ezsync=ezSync.main:main',
        ],
    },
    author="EZWave Team",
    author_email="info@ezwave.com",
    description="A Tarana Radio Management Tool",
    keywords="tarana, radio, management, ezwave",
    python_requires=">=3.6",
) 