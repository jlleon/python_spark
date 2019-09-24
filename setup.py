from setuptools import setup, find_packages
 
setup(
    name = "consolidador",
    version = "0.1",
    description = "Consolidador encargado de leer los datos de balances y generar los resultados de captaciones, colocaciones",
    author = "Jorge León",
    packages = find_packages(),
    entry_points = {
        'console_scripts': [
            'consolidador = consolidador.__main__:main'
        ]
    }
)