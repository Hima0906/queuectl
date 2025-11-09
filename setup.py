from setuptools import setup, find_packages

setup(
    name="queuectl",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'click>=8.0.0',
        'SQLAlchemy>=1.4.0',
        'python-dateutil>=2.8.0',
        'tabulate>=0.8.0',
        'pywin32>=300; sys_platform == "win32"',
    ],
    entry_points={
        'console_scripts': [
            'queuectl=queuectl.cli:cli',
        ],
    },
    python_requires='>=3.7',
)
