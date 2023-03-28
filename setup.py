import setuptools
import pkg_resources
import shutil
from pathlib import Path


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
    install_requires=requirements,
    package_data={'ysp_bot': ['config.yaml', 'data/*']},
    include_package_data=True
)


import yaml
config_path = Path(pkg_resources.resource_filename('ysp_bot', 'config.yaml'))
data_path = Path(pkg_resources.resource_filename('ysp_bot', 'data'))
with open(config_path) as f:
    dump_dir = Path(yaml.safe_load(f)['local']['data']).expanduser() / 'dump'
dump_dir.mkdir(parents=True, exist_ok=True)
shutil.copytree(data_path / 'cave_506', dump_dir/ 'cave_506',
                dirs_exist_ok=True)
