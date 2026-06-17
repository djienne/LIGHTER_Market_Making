"""Build script for Cython extensions.

Usage:
    python setup_cython.py build_ext --inplace
"""

import os

from setuptools import setup, Extension
from Cython.Build import cythonize

extra_compile_args = ["-O3", "-march=native"]
if os.getenv("CYTHON_FAST_MATH") == "1":
    # Benchmark-only: -ffast-math can fold isfinite() checks away.
    extra_compile_args.append("-ffast-math")

extensions = [
    Extension(
        "_vol_obi_fast",
        ["_vol_obi_fast.pyx"],
        extra_compile_args=extra_compile_args,
    ),
]

setup(
    name="vol_obi_cython",
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            "language_level": "3",
            "boundscheck": False,
            "wraparound": False,
            "cdivision": True,
            "initializedcheck": False,
        },
    ),
)
