from setuptools import setup

if __name__ == "__main__":
    setup(
        cffi_modules=["./pfstatsd/ifstats_build.py:ffibuilder"],
        zip_safe=False,
    )
