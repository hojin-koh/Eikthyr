import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Eikthyr",
    version="0.0.1",
    author="Hojin KOh",
    author_email="hojin-koh@users.noreply.github.com",
    description="Scientific Extensions of luigi",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hojin-koh/Eikthyr",
    project_urls={
        "Bug Tracker": "https://github.com/hojin-koh/Eikthyr/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=["luigi==3.0.3"],
)
