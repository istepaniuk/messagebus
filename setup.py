from setuptools import setup

setup(
    name='messagebus',
    version="1.0.3",
    author='Ivan Stepaniuk',
    author_email='istepaniuk+mb@gmail.com',
    description='Wrapper arround Pika to publish and subscribe domain events',
    url='https://github.com/istepaniuk/messagebus',
    packages=['messagebus'],
    install_requires=open('requirements.txt', 'r').readlines(),
    include_package_data=True,
    long_description=open('README.md').read(),
)
