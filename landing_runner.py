# import runpy

# runpy.run_module("pipeline.landing.rontec", run_name="__main__")

import importlib
import pkgutil
# Dynamic Running
import runpy
import time


def run_landing_modules(package_name):
    package = importlib.import_module(package_name)
    for _, module_name, _ in pkgutil.iter_modules(package.__path__):
        full_module_name = f"{package_name}.{module_name}"

        print(f"Running module: {full_module_name}")
        try:
            runpy.run_module(full_module_name, run_name="__main__")
        except:
            pass
        time.sleep(1)


if __name__ == "__main__":
    # Replace 'pipeline' with the name of your top-level package
    run_landing_modules("pipeline.landing")
