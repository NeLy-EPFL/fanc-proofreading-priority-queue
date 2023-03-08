import setuptools

with open('requirements.txt') as f:
    requirements = f.read().splitlines()
    requirements = [l for l in requirements if not l.strip().startswith('#')]

setuptools.setup(
    name='you-should-proofread-bot',
    version='0.0.1',
    description='Prioritizing segments to proofread in FANC',
    author='Sibo Wang',
    author_email='sibo.wang@epfl.ch',
    packages=setuptools.find_packages(),
    install_requires=requirements
)
