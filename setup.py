from setuptools import setup

setup(
    name='messagebus',
    version='1.1.0',
    author='Ivan Stepaniuk',
    author_email='istepaniuk@gmail.com',
    description='Wrapper arround Pika to publish and subscribe domain events',
    long_description=open('README.md').read(),
    url='https://github.com/istepaniuk/messagebus',
    classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 3'
    ],
    license='MIT/X11',
    packages=['messagebus'],
    install_requires=['pika>=0.12', 'pyrabbit'],
    test_require=['mamba>=0.8', 'expect'],
    include_package_data=True,
)
