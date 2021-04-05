
NAME				=	parquet_cpp_php


INI_DIR				=	/etc/php5/conf.d


EXTENSION_DIR		=	$(shell php-config --extension-dir)


EXTENSION 			=	${NAME}.so
INI 				=	${NAME}.ini


COMPILER			=	g++
LINKER				=	g++


INCLUDE_DIR 		= 	"../arrow/cpp/src/"
LIB_DIR				= 	"../arrow/cpp/build/release"
COMPILER_FLAGS		=	-I${INCLUDE_DIR} -Wall -c -O2 -std=c++11 -fpic -o 
LINKER_FLAGS		=	-shared -L"/usr/local/lib/"
LINKER_DEPENDENCIES	=	-lphpcpp -larrow -lparquet


#
#	Command to remove files, copy files and create directories.
#
#	I've never encountered a *nix environment in which these commands do not work. 
#	So you can probably leave this as it is
#

RM					=	rm -f
CP					=	cp -f
MKDIR				=	mkdir -p


#
#	All source files are simply all *.cpp files found in the current directory
#
#	A builtin Makefile macro is used to scan the current directory and find 
#	all source files. The object files are all compiled versions of the source
#	file, with the .cpp extension being replaced by .o.
#

SOURCES				=	$(wildcard *.cpp)
OBJECTS				=	$(SOURCES:%.cpp=%.o)


#
#	From here the build instructions start
#

all:					${OBJECTS} ${EXTENSION}

${EXTENSION}:			${OBJECTS}
						${LINKER} ${LINKER_FLAGS} -o $@ ${OBJECTS} ${LINKER_DEPENDENCIES}

${OBJECTS}:
						${COMPILER} ${COMPILER_FLAGS} $@ ${@:%.o=%.cpp}

install:		
						${CP} ${EXTENSION} ${EXTENSION_DIR}
						${CP} ${INI} ${INI_DIR}
				
clean:
						${RM} ${EXTENSION} ${OBJECTS}
