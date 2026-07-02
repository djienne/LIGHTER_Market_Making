"""Build script for Cython extensions.

Usage:
    python setup_cython.py build_ext --inplace

Environment:
    CYTHON_MARCH      Target microarchitecture for -march (default: native).
                      Docker images should use a portable baseline such as
                      x86-64-v3 — baking the build host's ISA into an image
                      via -march=native can SIGILL on a different deploy host.
    CYTHON_FAST_MATH  Set to 1 to add -ffast-math (benchmark-only).
"""

import os
import sys

from setuptools import setup, Extension
from Cython.Build import cythonize

_march = os.getenv("CYTHON_MARCH", "native")

# MSVC ignores GCC-style flags (emitting D9002 warnings and producing an
# unoptimized-flag build); use its own switches.  MinGW builds pass
# --compiler=mingw32 and take the GCC branch.
_using_msvc = sys.platform == "win32" and "mingw32" not in " ".join(sys.argv)
if _using_msvc:
    extra_compile_args = ["/O2"]
else:
    extra_compile_args = ["-O3", f"-march={_march}"]
    if sys.platform == "win32" and sys.maxsize > 2**32:
        # 64-bit MinGW needs MS_WIN64 or pyconfig.h picks the wrong
        # SIZEOF_VOID_P and the build fails its pointer-size sanity check.
        extra_compile_args.append("-DMS_WIN64")
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
