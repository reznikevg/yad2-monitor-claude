from setuptools import setup

setup(
    name="playwright-noop",
    version="1.52.0",
    py_modules=["noop_pw"],
    entry_points={
        "console_scripts": [
            "playwright=noop_pw:main",
        ],
    },
)
