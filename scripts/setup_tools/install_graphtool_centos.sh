'''
yum install cairomm cairomm-devel pycairo pycairo-devel sparsehash sparsehash-devel python-pip -y


wget https://sourceforge.net/projects/boost/files/boost/1.56.0/boost_1_56_0.tar.gz
tar xf boost_1_56_0.tar.gz
cd boost_1_56_0
./bootstrap.sh
./b2 install --prefix=/usr/local/apps/boost-1.56.0
export CPLUS_INCLUDE_PATH=$CPLUS_INCLUDE_PATH:/usr/local/apps/boost-1.56.0/include/
export C_INCLUDE_PATH=$C_INCLUDE_PATH:/usr/local/apps/boost-1.56.0/include/
export LD_LIBRARY_PATH=/usr/local/apps/boost-1.56.0/lib/:$LD_LIBRARY_PATH
cd

wget https://github.com/CGAL/cgal/releases/download/releases%2FCGAL-4.7/CGAL-4.7.tar.gz
tar zxf CGAL-4.7.tar.gz 
cd CGAL-4.7
mkdir /usr/local/apps/cgal-4.8
cmake -DBoost_INCLUDE_DIR=/usr/local/apps/boost-1.56.0/include/ -DCMAKE_INSTALL_PREFIX=/usr/local/apps/cgal-4.8/ .
make
ln -s /usr/local/apps/cgal/lib/libCGAL.so /usr/local/lib64/
cd

mkdir -p /usr/local/apps/openblas

git clone https://github.com/xianyi/OpenBLAS
cd OpenBLAS
make FC=gfortran
make PREFIX=/usr/local/apps/openblas/ install
export LD_LIBRARY_PATH=/usr/local/apps/openblas/lib:$LD_LIBRARY_PATH
cd

git clone https://github.com/numpy/numpy
cd numpy
echo "[default]
include_dirs = /usr/local/apps/openblas/include
library_dirs = /usr/local/apps/openblas/lib

[openblas]
openblas_libs = openblas
library_dirs = /usr/local/apps/openblas/lib

[lapack]
lapack_libs = openblas
library_dirs = /usr/local/apps/openblas/lib" >> site.cfg
python setup.py build --fcompiler=gnu95
python setup.py 

cd
pip install scipy matplotlib
'''

wget https://downloads.skewed.de/graph-tool/graph-tool-2.10.tar.bz2
tar xf graph-tool-2.10.tar.bz2 
cd graph-tool-2.10
CPPFLAGS='-I/usr/local/apps/cgal-4.7/include/ -I/usr/include/google/ -I/usr/local/apps/boost-1.56.0/include/'  LDFLAGS="-L/usr/local/apps/boost-1.60.0/lib/"  ./configure --enable-openmp --with-sparsehash-prefix=google
make -j 4
make install
