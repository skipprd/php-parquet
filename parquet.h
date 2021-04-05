/*! \file       parquet.h 
    \brief      The parquet class declaration, gives possibilty to read from and write into a parquet file.
    \author     Malek Atia @github.com/Malek-ATIA
    \version    1.0
    \date       02.2021
*/

#ifndef PARQUET_H
#define PARQUET_H

#include "tools.h"


template <typename T, typename U, typename V>
void reader(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col);

template <>
void reader<parquet::Int96Reader*, parquet::Int96, Php::Value>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col);

template <>
void reader<parquet::ByteArrayReader*, parquet::ByteArray, Php::Value>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col);

template <>
void reader<parquet::FixedLenByteArrayReader*, parquet::FixedLenByteArray, Php::Value>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col);

parquet::Type::type get_parquet_type(Php::Value value);

template <typename T, typename U, typename V>
void reader(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col);

template <>
void reader<parquet::Int96Reader*, parquet::Int96, Php::Value>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col) ;

template <>
void reader<parquet::ByteArrayReader*, parquet::ByteArray, Php::Value>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col);

template <>
void reader<parquet::FixedLenByteArrayReader*, parquet::FixedLenByteArray, Php::Value>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col);


class Parquet : public Php::Base
{

private:

    /*! \var file_name
	    \brief Path of the parquet file to be read
    */
    std::string file_name;
    /*! \var parquet_file_reader
        \brief Parquet File reader
    */
    std::unique_ptr<parquet::ParquetFileReader> parquet_file_reader;
    /*! \var column_readers
        \brief Parquet Column reader
    */
    std::vector<std::shared_ptr<parquet::ColumnReader>> column_readers;

    /*! \var int nbr_columns 
        \brief The number of columns read from the schema, defines the number elementary colmumns in the schema. 
    */
    
    int nbr_columns = 0;
    /*! \var int nbr_real_columns 
        \brief The number of real columns read from the schema, defines the number colmumns in 1st level in the schema. 
    */
    int nbr_real_columns = 0;

    /*!
        File output stream to be used to write in the parquet file
    */
    using FileClass = ::arrow::io::FileOutputStream;
    
    /*! \var parquet_writer
        \brief The actual parquet writer
     */
    std::shared_ptr<parquet::ParquetFileWriter> parquet_writer;
    
    /*! \var file_writer
        \brief The actual file writer
    */
    std::shared_ptr<FileClass> file_writer;

    /*! \var rgw
        \brief the row group writer
    */
    parquet::RowGroupWriter* rgw;
    
public:

    Parquet();

    ~Parquet();

    void create_writer(Php::Parameters& params);

    void create_reader(Php::Parameters& params);

    void read_col_int(int col, Php::Array& ar_col);

    void read_col(Php::Parameters& params);

    void read_element(Php::Parameters& params);

    void Write(Php::Parameters& params);

    NodePtr SetupSchema(std::string root_name, parquet::Repetition::type root_repetition, Php::Array schema, parquet::ConvertedType::type converted_type = parquet::ConvertedType::NONE);

    void get_file_json(Php::Parameters& params);

    void walkSchema(const NodePtr& node, Php::Value &res);

    Php::Value getInfo();

    void close_writer();

    void close_reader();

};

#endif
