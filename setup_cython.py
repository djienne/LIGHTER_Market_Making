"""Build script for Cython extensions.

Usage:
    python setup_cython.py build_ext --inplace
"""

from setuptools import setup, Extension
from Cython.Build import cythonize

extensions = [
    Extension(
        "_vol_obi_fast",
        ["_vol_obi_fast.pyx"],
        extra_compile_args=["-O3", "-march=native", "-ffast-math"],
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
