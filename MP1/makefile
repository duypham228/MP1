all: crsd.o crc.o
	g++ -pthread -o crsd crsd.o
	g++ -pthread -o crc crc.o

crc.o: crc.c interface.h
	g++ -g -c crc.c

crsd.o: crsd.c
	g++ -g -c crsd.c

clean: 
	rm *.o