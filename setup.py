from setuptools import setup, find_packages

setup(
    name="wind_turbines_project",
    version="0.1.2",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark>=3.2.0",
        "delta-spark>=2.1.0",
        # Add other dependencies from requirements.txt
    ],
    entry_points={
        "console_scripts": [
            "run_pipeline=my_pipeline.pipeline:main",
        ],
    },
    python_requires=">=3.8",
)
