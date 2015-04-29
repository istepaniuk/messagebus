from setuptools import setup

setup(
    name='messagebus',
    version="1.0.1",
    author='The Cloakroom',
    author_email='technical@thecloakroom.com',
    description='Wrapper arround Pika to publish and subscribe domain events',
    url='https://github.com/TheCloakroom/messagebus',
    packages=['messagebus'],
    install_requires=open('requirements.txt', 'r').readlines(),
    include_package_data=True,
    long_description=open('README.md').read(),
)
