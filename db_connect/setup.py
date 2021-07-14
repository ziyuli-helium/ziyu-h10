import setuptools

setuptools.setup(
    name="db_connect", # Replace with your own username
    version="0.0.1",
    author="Ziyu Li",
    author_email="ziyu.l@helium10.com",
    description="package for h10 db connection",
    url="https://github.com/ziyuli-helium/ziyu-h10/db_connect",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    packages=setuptools.find_packages(),
    install_requires = ['boto3','pandas','psycopg2','mysql.connector','sshtunnel','json']
    python_requires='>=3.6'
)
