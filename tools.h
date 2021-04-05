/*! \file       tools.h
    \brief      Contains the declarations of the tools needed for the parquet class.
    \author     Malek Atia @github.com/Malek-ATIA
    \version    1.0
    \date       02.2021
*/

#include <phpcpp.h>

#include <arrow/io/file.h>
#include <arrow/util/logging.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <vector>
#include <iostream>
#include <string>
#include <map>


#define DEBUG 1


using namespace Php;

using parquet::Compression;
using parquet::LogicalType;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::PrimitiveNode;
using parquet::schema::GroupNode;
using parquet::schema::NodePtr;
using parquet::schema::Node;
using parquet::WriterProperties;
using parquet::ConvertedType;


Php::Array get_keys(Php::Array arr);

bool is_undefined(Php::Value attr);

#if DEBUG
void print_array_json(Php::Array arr, int spaces);

void flatten_nested_array(Php::Array nested, Php::Array& array_1d, int& index_array_1d);

void arrange_input_arrays(Php::Array input_array, Php::Array& output_array);
#endif

void write_bool(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required);

void write_int32(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required);

void write_int64(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required);

void write_int96(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required);

void write_float(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required);

void write_double(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required);

void write_byte_array(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required);

void write_flba(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required);


Php::Value get_value_from_vector_path(Php::Value row, std::vector<std::string> vPath, int index);

void get_keys_values(Php::Array Map, Php::Array& Keys, Php::Array& Values);

parquet::ConvertedType::type get_converted_type(Php::Value value);

parquet::Repetition::type get_reptition_type(Php::Value value);
