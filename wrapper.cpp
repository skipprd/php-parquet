/*! \file       wrapper.cpp
    \brief      The cpp-php extension function.
    \author     Malek Atia @github.com/Malek-ATIA
    \version    1.0
    \date       2021
*/

#include "parquet.h"
#include "tools.h"

// Symbols are exported according to the "C" language
extern "C" 
{
    /*! \fn PHPCPP_EXPORT void *get_module()
        \brief export the "get_module" function that will be called by the Zend engine
    */
    PHPCPP_EXPORT void *get_module()
    {
        static Php::Extension extension("parquet_cpp");
        
        /*! 
            Exports the C++ class to a PHP class with the name input as parameter
        */
        Php::Class<Parquet> parquet("Parquet");

                // description of the class so that PHP knows which methods are accessible
        Php::Class<Parquet> reader("Parquet");
        parquet.method<&Parquet::create_reader> ("create_reader");
        parquet.method<&Parquet::get_file_json> ("json_file", {
            Php::ByRef("json", Php::Type::String, true)
        });
        parquet.method<&Parquet::getInfo> ("getInfo");
        parquet.method<&Parquet::close_reader> ("close_reader");
        parquet.method<&Parquet::read_col> ("read_col", {
            Php::ByVal("col", Php::Type::Numeric, true),
            Php::ByRef("val", Php::Type::Object, true)
        });
        parquet.method<&Parquet::read_element> ("read_element", {
            Php::ByVal("row", Php::Type::Numeric, true),
            Php::ByVal("col", Php::Type::Numeric, true),
            Php::ByRef("val")
        });

        /*! 
            Add the construct method to the PHP exported ParquetWriter class
            param#1 Name of the PHP method
            param#2 File name should be passed by value
            param#3 Schema array should be passed by value
        */
        parquet.method<&Parquet::create_writer> ("create_writer", {
            Php::ByVal("file_name", Php::Type::String, true),
            Php::ByVal("schema", Php::Type::Array, true)
        });

        /*! 
            Add the close mthod to the PHP exported ParquetWriter class
        */
        parquet.method<&Parquet::close_writer> ("close_writer");
        /*! 
            Add the write method to the PHP exported ParquetWriter class
            param#1 The array of values array to be inserted into the parquetfile, should be passed by value
            param#2 The number of rows written: should be passed by reference. 
        */

        parquet.method<&Parquet::Write> ("write", {
            Php::ByVal("val", Php::Type::Array, true)
        });

        // add the class to the extension
        extension.add(std::move(parquet));
        
        // return the extension module
        return extension;
    }
}
