CXX=g++
CXXFLAGS=-g -O3
LIB=-L../lib -lengine -lpthread -lrt -lz -lpmem
STATIC_LIB=/usr/local/lib/libgflags.a

bench:
	${CXX} ${CXXFLAGS} -o bench -I.. -I../include bench.cpp ${LIB} ${STATIC_LIB}
	${CXX} ${CXXFLAGS} -o test -I.. -I../include test.cpp ${LIB} ${STATIC_LIB}


clean:
	rm bench test
