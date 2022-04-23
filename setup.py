import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="minumtium_sqlite",
    version="1.0.2",
    author="Danilo Guimaraes (danodic)",
    author_email="danilo@danodic.dev",
    description="A SQLite adapter for the minumtium library.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/danodic-dev/minumtium-sql-alchemy",
    project_urls={
        "Bug Tracker": "https://github.com/danodic-dev/minumtium-sql-alchemy/issues",
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(),
    install_requires=['sqlalchemy', 'minumtium', 'pydantic'],
    python_requires=">=3.6",
)
