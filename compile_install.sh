#!/bin/bash

PARQUET_EXTENSION_NAME="parquet_cpp_php.so"

if [[ "$OSTYPE" == "darwin"* ]]
then
    PHP_CONFIG_FILE_PATH="/usr/local/etc/php/7.4/php.ini"
    PHP_EXTENSION_FOLDER_PATH="/usr/local/lib/php/pecl/20190902/"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    PHP_CONFIG_FILE_PATH="/etc/php/7.4/cli/php.ini"
    PHP_EXTENSION_FOLDER_PATH="/usr/lib/php/20190902/"
fi

PHP_CPP_DIR='lib_php_cpp'
PHP_CPP_GIT_LINK='https://github.com/CopernicaMarketingSoftware/PHP-CPP.git'


PHP_PARQUET_EXTENSION_LOADED=$(grep "extension="$PARQUET_EXTENSION_NAME $PHP_CONFIG_FILE_PATH | xargs)

install_arrow_parquet() {
    #Download arrow from git
    git clone https://github.com/apache/arrow.git
    cd arrow/cpp
    
    #prepare make file
    cmake . -DARROW_PARQUET=ON \
      -DARROW_OPTIONAL_INSTALL=ON \
      -DARROW_BUILD_TESTS=ON \
      -DARROW_WITH_SNAPPY=ON
    
    #compile arrow and parquet
    make -j8 arrow
    make -j8 parquet
   
    #install libs
    sudo make install

    cd ../../
}

#Donwload lib-php-cpp
install_php_cpp_linux() {
    mkdir $PHP_CPP_DIR
    git clone $PHP_CPP_GIT_LINK $PHP_CPP_DIR
    #Compile
    cd $PHP_CPP_DIR
    make -j8
    #install
    sudo make install
    cd ..
}

install_php_cpp_mac() {
    #Unlik any newer php versions
    brew unlink php
    #install php 7.2
    brew install php@7.4
    #link to php72
    brew link php@7.4
    #Ordinary install
    install_php_cpp_linux
}

compile_extensions() {
    #Compile parquet binding
    cd "src"
    make clean
    make -j8
    cd ..
}

install_load_extensions() {

    #Update the php configuration file to include the extensions
    #Check if those extensions aren't loaded already?
    if [ "$PHP_PARQUET_EXTENSION_LOADED" == "" ]
    then
        sudo echo "extension="$PARQUET_EXTENSION_NAME >> $PHP_CONFIG_FILE_PATH
    fi
    
    #Copy the extensions file to the correct directory
    sudo cp "./src/"$PARQUET_EXTENSION_NAME $PHP_EXTENSION_FOLDER_PATH
    
    #Restart Apache
    if [[ "$OSTYPE" == "linux-gnu"* ]]
    then
        sudo service apache2 restart
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        sudo apachectl restart
    fi
}

clean_install_files() {
    
    cd src/
    make clean
    cd ..
}

main() {

    #install_arrow_parquet

    #if [[ "$OSTYPE" == "darwin"* ]]
    #then
    #    install_php_cpp_mac
    #elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    #    install_php_cpp_linux
    #fi

    compile_extensions
    install_load_extensions
    clean_install_files
}


#Run script.
main
