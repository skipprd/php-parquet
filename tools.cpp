/*! \file       tools.cpp
    \brief      Tool box source file, contains some functions for the use of the parquet class.
    \author     Malek Atia @github.com/Malek-ATIA
    \version    1.0
    \date       02.2021
*/

#include "tools.h"

/*! \fn parquet::Type::type get_parquet_type(Php::Value value)
    \brief Returns the parquet type of the value entered.
*/
parquet::Type::type get_parquet_type(Php::Value value) {

    std::string value_str = value.stringValue();
    parquet::Type::type parquet_type = parquet::Type::BOOLEAN;
    
    if (value_str.compare("bool") == 0) {
        parquet_type = parquet::Type::BOOLEAN;
    } else if (value_str.compare("int32") == 0) {
        parquet_type = parquet::Type::INT32;
    } else if (value_str.compare("int64") == 0) {
        parquet_type = parquet::Type::INT64;
    } else if (value_str.compare("timestamp") == 0) {
        parquet_type = parquet::Type::INT64;
    } else if (value_str.compare("int96") == 0) {
        parquet_type = parquet::Type::INT96;
    } else if (value_str.compare("float") == 0) {
        parquet_type = parquet::Type::FLOAT;
    } else if (value_str.compare("double") == 0) {
        parquet_type = parquet::Type::DOUBLE;
    } else if (value_str.compare("string") == 0) {
        parquet_type = parquet::Type::BYTE_ARRAY;
    } else if (value_str.compare("byte_array") == 0) {
        parquet_type = parquet::Type::BYTE_ARRAY;
    } else if (value_str.compare("fixed_len_byte_array") == 0) {
        parquet_type = parquet::Type::FIXED_LEN_BYTE_ARRAY;
    }

    return parquet_type;
}


/*! \fn Php::Array get_keys(Php::Array arr)
    \brief Extracts the keys values of any map array.
    \param arr the map array
*/
Php::Array get_keys(Php::Array arr) {

    Php::Array keys;
    int index_array = 0;

    for (auto &iter : arr) {
        Php::Value key = iter.first;
        keys[index_array++] = key;
    }
    return keys;
}


/*! \fn bool is_undefined(Php::Value attr)
    \brief Checks if the attribute is undefined or not
    \param attr the attribute to be checked.
*/
bool is_undefined(Php::Value attr) {

    if(attr.isNull()) return true;

    return false;
}

#if DEBUG
int num_spaces = 0;

/*! \fn void print_array_json(Php::Array arr, int spaces)
    \brief For debug purposes, print an array in json format for better visibility
    \param arr The array to be printted
    \param spaces The number of whitespaces between array levels.
*/
void print_array_json(Php::Array arr, int spaces) {

    for (auto &iter : arr) {
        Php::Value key = iter.first;
        Php::Value value = iter.second;
        for (int j = 0; j<spaces; j++) printf(" ");
            printf("%s =====> ", key.stringValue().c_str());
        
        if (value.isArray()) {
            printf("\n");
            print_array_json((Php::Array) value, spaces * 2);
        }
        else  {
            for (int j = 0; j < spaces/2; j++) printf(" ");
            printf("%s\n", value.stringValue().c_str());
        }
    }
}

/*! \fn void flatten_nested_array(Php::Array nested, Php::Array& array_1d, int& index_array_1d)
    \brief For debug purposes, flatten/serialize any complex/nested array to 1-D array
    \param nested The input nested array
    \param array_1d The output 1-D array
    \param index_array_1d This index of the element in the output array
*/
void flatten_nested_array(Php::Array nested, Php::Array& array_1d, int& index_array_1d) {

    for (auto &iter : nested) {
        if(iter.second.isArray())
            flatten_nested_array(iter.second, array_1d, index_array_1d);
        else {
            array_1d[index_array_1d] = iter.second;
            index_array_1d++;
        }
    }
}

#if 0
/*! \fn void arrange_input_arrays(Php::Array input_array, Php::Array& output_array).
    \brief For debug purposes, gather all columns together, might be useful in some cases.
    \param input_array The nested array which contains an array of rows.
    \param output_array The output array which contains an array of columns.
*/
void arrange_input_arrays(Php::Array input_array, Php::Array& output_array) {

    // We must serialize all rows and then
    // Put columns with same index together
    // meaning.. [['dog', 'true', [1, 2]], ['cat', 'false', [0, 3]]]
    // ==> ['dog', 'cat', 'true', 'false', 1, 0, 2, 3]

    int nbr_rows = input_array.size();
    Php::Array serialized_arrays;
    
    // Serialize row and add it to serialized_arrays!
    for (int i = 0; i < nbr_rows; i++) {
        int index_insid_array = 0;
        Php::Value input_aray_at_index = input_array[i];
        Php::Array serilized_array_at_index;
        flatten_nested_array(input_aray_at_index, serilized_array_at_index, index_insid_array);
        serialized_arrays[i] = serilized_array_at_index;
    }

    for (int i = 0; i < nbr_rows; i++) {
        Php::Value ar = serialized_arrays[i];
        //print_array_json(ar, 2);
    }

    int k = 0;
    // Now Append columns like together
    for (int i = 0; i < nbr_columns; i++) {
        for (int j = 0; j < nbr_rows; j++) {
            Php::Value ar = serialized_arrays[j][i];
            output_array[k] = ar;
            k++;
        }
    }
}
#endif
#endif


/*! \fn static void write_bool(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required)
    \brief The bool value writer
    \param column_writer 
    \param val the bool value to be written
    \param def the definition value
    \param rep the repetition value
    \param is_required
     
*/
void write_bool(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required) {
    parquet::BoolWriter* writer = static_cast<parquet::BoolWriter*>(column_writer);
    bool input_value;
    bool* value = &input_value;
    int16_t zerodef = 0;
    int16_t* cdef = def;

    if (is_undefined(val)) {
        cdef = &zerodef;
        value = nullptr;
    } else {
        input_value = val.boolValue();
    }
    
    writer->WriteBatch(1, cdef, rep, value);
}

/*! \fn static void write_int32(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required)
    \brief The int32 value writer
    \param column_writer 
    \param val the int32 value to be written
    \param def the definition value
    \param rep the repetition value
    \param is_required
     
*/
void write_int32(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required) {
    parquet::Int32Writer* writer = static_cast<parquet::Int32Writer*>(column_writer);
    int32_t input_value;
    int32_t* value = &input_value;
    int16_t zerodef = 0;
    int16_t* cdef = def;

    if (is_undefined(val)) {
        cdef = &zerodef;
        value = nullptr;
    } else {
        input_value = val.numericValue();
    }

    writer->WriteBatch(1, cdef, rep, value);
}

/*! \fn static void write_int64(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required)
    \brief The int64 value writer
    \param column_writer 
    \param val the int64 value to be written
    \param def the definition value
    \param rep the repetition value
    \param is_required
     
*/
void write_int64(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required) {
    parquet::Int64Writer* writer = static_cast<parquet::Int64Writer*>(column_writer);
    int64_t input_value;
    int64_t* value = &input_value;
    int16_t zerodef = 0;
    int16_t* cdef = def;

    if (is_undefined(val)) {
        cdef = &zerodef;
        value = nullptr;
    } else {
        input_value = val.numericValue();
    }
    writer->WriteBatch(1, cdef, rep, value);
}

/*! \fn static void write_int96(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required)
    \brief The int96 value writer
    \param column_writer 
    \param val the int96 value to be written
    \param def the definition value
    \param rep the repetition value
    \param is_required
     
*/
void write_int96(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required) {
    parquet::Int96Writer* writer = static_cast<parquet::Int96Writer*>(column_writer);
    parquet::Int96 input_value;
    parquet::Int96* value = &input_value;
    int16_t zerodef = 0;
    int16_t* cdef = def;

    if (is_undefined(val)) {
        cdef = &zerodef;
        value = nullptr;
    } else {
        uint32_t* buf = (uint32_t*) val.stringValue().c_str();

        input_value.value[0] = buf[0];
        input_value.value[1] = buf[1];
        input_value.value[2] = buf[2];
    }
    writer->WriteBatch(1, cdef, rep, value);
}

/*! \fn static void write_float(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required)
    \brief The float value writer
    \param column_writer 
    \param val the float value to be written
    \param def the definition value
    \param  the repetition value
    \param is_required
     
*/
void write_float(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required) {
    parquet::FloatWriter* writer = static_cast<parquet::FloatWriter*>(column_writer);
    float input_value;
    float* value = &input_value;
    int16_t zerodef = 0;
    int16_t* cdef = def;

    if (is_undefined(val)) {
        cdef = &zerodef;
        value = nullptr;
    } else {
        input_value = val.floatValue();
    }

    writer->WriteBatch(1, cdef, rep, value);
}

/*! \fn static void write_double(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required)
    \brief The double value writer
    \param column_writer 
    \param val the double value to be written
    \param def the definition value
    \param rep the repetition value
    \param bool is_required
     
*/
void write_double(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required) {
    parquet::DoubleWriter* writer = static_cast<parquet::DoubleWriter*>(column_writer);
    double input_value;
    double* value = &input_value;
    int16_t zerodef = 0;
    int16_t* cdef = def;

    if (is_undefined(val)) {
        cdef = &zerodef;
        value = nullptr;
    } else {
        input_value = val.floatValue();
    }

    writer->WriteBatch(1, cdef, rep, value);

}

/*! \fn static void write_byte_array(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required)
    \brief The byte array value writer
    \param column_writer 
    \param val the byte array value to be written
    \param def the definition value
    \param rep the repetition value
    \param is_required
     
*/
void write_byte_array(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required) {

  parquet::ByteArrayWriter* writer = static_cast<parquet::ByteArrayWriter*>(column_writer);
    parquet::ByteArray input_value;
    parquet::ByteArray* value = &input_value;
    int16_t zerodef = 0;
    int16_t* cdef = def;
    
    if (val.isString()) {
        char* str_value = (char*) malloc ((sizeof(char*) * val.stringValue().length()));
        strncpy(str_value, val.stringValue().c_str(), val.stringValue().length());
//        int16_t definition_level = 1;
        input_value.ptr = reinterpret_cast<const uint8_t*>(&str_value[0]);
        input_value.len = val.stringValue().length();
    } else if (is_undefined(val)) {
        if (is_required)
            ;//Throw Php exception: a byte array value is required

        cdef = &zerodef;
        value = nullptr;
    } else {
        //Throw Php exception: Parameter is not a byte array
        throw Php::Exception("Parameter is not a byte array");
        return;
    }

    writer->WriteBatch(1, cdef, rep, value);
}

/*! \fn static void write_flba(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required)
    \brief The fixed length byte array value writer
    \param column_writer 
    \param val the fixed length byte array value to be written
    \param def the definition value
    \param rep the repetition value
    \param is_required
     
*/
void write_flba(parquet::ColumnWriter* column_writer, Php::Value val, int16_t* def, int16_t* rep, bool is_required) {
    parquet::FixedLenByteArrayWriter* writer = static_cast<parquet::FixedLenByteArrayWriter*>(column_writer);
    parquet::FixedLenByteArray input_value;
    parquet::FixedLenByteArray* value = &input_value;
    int16_t zerodef = 0;
    int16_t* cdef = def;


    if (is_undefined(val)) {
        cdef = &zerodef;
        value = nullptr;
    } else {
        Php::Object obj_value = val;
        input_value.ptr = reinterpret_cast<const uint8_t*>(val.rawValue());        
    }
    writer->WriteBatch(1, cdef, rep, value);

}

/*!
    Typedef writer
*/
typedef void (*writer_t)(parquet::ColumnWriter*, Php::Value, int16_t*, int16_t*, bool);

/*! \fn Php::Value get_value_from_vector_path(Php::Value row, std::vector<std::string> vPath, int index = 0)
    \brief To get The value of the path field from the row array
    \param row array from where we find the value
    \param vPath The path of searchable value
    \param index Index in the vPath array

*/
Php::Value get_value_from_vector_path(Php::Value row, std::vector<std::string> vPath, int index = 0) {

    int vPathIndex = vPath.size() - 1;
    if (index < vPathIndex) {
        Php::Value rowX = row.get(vPath.at(index));
        int index_plus_1 = index + 1;
        return get_value_from_vector_path(row.get(vPath.at(index)), vPath, index_plus_1);   
    } else {
        return row.get(vPath.at(index));
    }
}

/*! \fn void get_keys_values(Php::Array Map, Php::Array& Keys, Php::Array& Values)
    \brief Extracts the keys/values arrays from an array of maps
    \param Map The input array
    \param Keys The output array of the keys
    \param Values The output array of the values
*/
void get_keys_values(Php::Array Map, Php::Array& Keys, Php::Array& Values) {

    int i = 0;

    for (auto &iter : Map) {
        Php::Value key = iter.first;
        Keys.set(i, key);

        Php::Value value = iter.second;
        Values.set(i, value);

        i++;
    }    
}


/*! \fn parquet::ConvertedType::type get_converted_type(Php::Value value).
    \brief Returns the converted type of the value entered.
    \param value
*/
parquet::ConvertedType::type get_converted_type(Php::Value value) {

    std::string value_str = value.stringValue();
    parquet::ConvertedType::type converted_type = parquet::ConvertedType::type::NONE;

    if (value_str.compare("timestamp") == 0)
        converted_type = parquet::ConvertedType::TIMESTAMP_MILLIS;
    else if (value_str.compare("string") == 0)
        converted_type = parquet::ConvertedType::UTF8;

    return converted_type;
}


/*! \fn parquet::Repetition::type get_reptition_type(Php::Value value).
    \brief Returns the repetition type of the value entered.
    \param value
*/
parquet::Repetition::type get_reptition_type(Php::Value value) {
       
    if (value.numericValue() == 0) return Repetition::REQUIRED;
    else if (value.numericValue() == 1) return Repetition::OPTIONAL;
    else return Repetition::REPEATED;
    
}
