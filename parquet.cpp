/*! \file       parquet.cpp
    \brief      The parquet class definition, gives possibilty to read from and write into a parquet file.
    \author     Malek Atia @github.com/Malek-ATIA
    \version    1.0
    \date       02.2021
*/

#include "tools.h"
#include "parquet.h"


/*! \fn void reader(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col).
    \brief Template column reader.
    \param column_reader
    \param maxdef
    \param maxrep
    \param ar_col
*/
template <typename T, typename U, typename V>
void reader(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col) {

    T reader = static_cast<T>(column_reader.get());

    Php::Array array;
    Php::Value RowVal;
    std::vector<Php::Value> ColValues;

    while(reader->HasNext())
    {
        U value;
        int64_t value_read;
        int16_t definition = maxdef;
        int16_t repetition = maxrep;

        reader->ReadBatch(1, &definition, &repetition, &value, &value_read);
       
        RowVal[0] = definition;
        RowVal[1] = repetition;
        RowVal[2] = value;

        ColValues.push_back(value);
    }

    Php::Array pArray = ColValues;
    ar_col = pArray;
}

/*! \fn void reader<parquet::Int96Reader*, parquet::Int96, Php::Value>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col)
    \brief Reads the type int96
*/
template <>
void reader<parquet::Int96Reader*, parquet::Int96, Php::Value>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col) {

    parquet::Int96Reader* reader = static_cast<parquet::Int96Reader*>(column_reader.get());

    Php::Array array;
    Php::Value RowVal;
    std::vector<Php::Value> ColValues;

    while(reader->HasNext())
    {
        parquet::Int96 value;
        int64_t value_read;
        int16_t definition = maxdef;
        int16_t repetition = maxrep;

        reader->ReadBatch(1, &definition, &repetition, &value, &value_read);

        RowVal[0] = definition;
        RowVal[1] = repetition;
        RowVal[2] = Php::Value((char*)value.value, 12);

        ColValues.push_back(Php::Value((char*)value.value, 12));
    }

    Php::Array pArray = ColValues;
    ar_col = pArray;
}

/*! \fn void reader<parquet::ByteArrayReader*, parquet::ByteArray, Pget_value_from_vector_path(Php::Value row, std::vector<hp::Value>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col)
    \brief Reads the type byte array
*/
template <>
void reader<parquet::ByteArrayReader*, parquet::ByteArray, Php::Value>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col) {

    parquet::ByteArrayReader* reader = static_cast<parquet::ByteArrayReader*>(column_reader.get());

    Php::Array array;
    Php::Value RowVal;
    std::vector<Php::Value> ColValues;
    
    while(reader->HasNext())
    {
        parquet::ByteArray value;
        int64_t value_read;
        int16_t definition = maxdef;
        int16_t repetition = maxrep;
        
        reader->ReadBatch(1, &definition, &repetition, &value, &value_read);

        RowVal[0] = definition;
        RowVal[1] = repetition;
        RowVal[2] = Php::Value((char*)value.ptr, value.len);

        ColValues.push_back(Php::Value((char*)value.ptr, value.len));
    }

    Php::Array pArray = ColValues;
    ar_col = pArray;
}

/*! \fn void reader<parquet::FixedLenByteArrayReader*, parquet::FixedLenByteArray, Php::Value>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col)
    \brief Reads the type fixed length byte array
*/
template <>
void reader<parquet::FixedLenByteArrayReader*, parquet::FixedLenByteArray, Php::Value>(std::shared_ptr<parquet::ColumnReader> column_reader, int16_t maxdef, int16_t maxrep, Php::Array& ar_col) {

    parquet::FixedLenByteArrayReader* reader = static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get());

    Php::Array array;
    Php::Value RowVal;
    std::vector<Php::Value> ColValues;

    while(reader->HasNext())
    {
        parquet::FixedLenByteArray value;
        int64_t value_read;
        int16_t definition = maxdef;
        int16_t repetition = maxrep;

        reader->ReadBatch(1, &definition, &repetition, &value, &value_read);

        RowVal[0] = definition;
        RowVal[1] = repetition;
        RowVal[2] = Php::Value((char*)value.ptr, 1);

        ColValues.push_back(Php::Value((char*)value.ptr, 1));
    }
    
    Php::Array pArray = ColValues;
    ar_col = pArray;
}


/*!  \typedef void (*reader_t)(std::shared_ptr<parquet::ColumnReader>, int16_t, int16_t, Php::Array& ar_col)
*/
typedef void (*reader_t)(std::shared_ptr<parquet::ColumnReader>, int16_t, int16_t, Php::Array& ar_col);

/*! \var reader_t type_readers[]
    \brief Table of parquet readers. Keep same order as in parquet::Type
*/
reader_t type_readers[] = {
    reader<parquet::BoolReader*, bool, Php::Value>,
    reader<parquet::Int32Reader*, int32_t, Php::Value>,
    reader<parquet::Int64Reader*, int64_t, Php::Value>,
    reader<parquet::Int96Reader*, parquet::Int96, Php::Value>,
    reader<parquet::FloatReader*, float, Php::Value>,
    reader<parquet::DoubleReader*, double, Php::Value>,
    reader<parquet::ByteArrayReader*, parquet::ByteArray, Php::Value>,
    reader<parquet::FixedLenByteArrayReader*, parquet::FixedLenByteArray, Php::Value>,
};


/*!
    Typedef writer
*/
typedef void (*writer_t)(parquet::ColumnWriter*, Php::Value, int16_t*, int16_t*, bool);

/*! \var  type_writers
    \brief Table of writer functions, keep same ordering as parquet::Type
*/
writer_t type_writers[] = {
    write_bool,
    write_int32,
    write_int64,
    write_int96,
    write_float,
    write_double,
    write_byte_array,
    write_flba
};

/*! \fn Parquet constructor method

*/
Parquet::Parquet() {
    #ifdef DEBUG
    printf("New Parquet cpp is created\n");
    #endif
}

/*! \fn Parquet desctructor method

*/
Parquet::~Parquet() {

}

/*! \fn create_writer(Php::Parameters &params) 
    \brief The real constructor of the class ParquetWriter
    \param params The params entered by the user: must contain at least 2 values
    1st value is the parquet file name to be written into.
    2nd value is the array containing the schema of the parquet file. 
*/
void Parquet::create_writer(Php::Parameters &params)
{
    // Arguments sanity checks
    if (params.size() < 2) {
        throw Php::Exception("[Parameters Error] Wrong number of arguments, must have at least 2");
        return;
    }
    if (!params[1].isArray()) {
        throw Php::Exception("[Parameters Error] Second argument is not an Array");
        return;
    }

    try {

        Php::Value str_file_name = params[0].stringValue();
        Php::Value str_compression = (params.size() <= 2) ? "" : params[2].stringValue();
        Php::Value arr_schema = params[1];
        std::shared_ptr<GroupNode> schema = std::static_pointer_cast<GroupNode>(SetupSchema("schema", Repetition::REQUIRED, arr_schema));

        PARQUET_ASSIGN_OR_THROW(file_writer, FileClass::Open(str_file_name.stringValue()));

        nbr_real_columns = get_keys(arr_schema).size();

        #ifdef DEBUG
        printf("Number of Real Columns = %d\n", nbr_real_columns);
        printf("Number of Columns = %d\n", nbr_columns);
        #endif
        
        parquet::Compression::type compression;

        std::string comp_str(str_compression.stringValue());
        if (comp_str.compare("snappy") == 0) {
            compression = parquet::Compression::SNAPPY;
        } else if (comp_str.compare("gzip") == 0) {
            compression = parquet::Compression::GZIP;
        } else if (comp_str.compare("lzo") == 0) {
            compression = parquet::Compression::LZO;
        } else if (comp_str.compare("brotli") == 0) {
            compression = parquet::Compression::BROTLI;
        } else if (comp_str.compare("undefined") == 0) {
            compression = parquet::Compression::UNCOMPRESSED;
        } else if (comp_str.compare("") == 0) {
            compression = parquet::Compression::UNCOMPRESSED;
        } else {
            
            throw Php::Exception("[Parameters Error] Wrong Compression type");
            return;
        }

        // Add writer properties
        parquet::WriterProperties::Builder builder;
        builder.compression(compression);

        std::shared_ptr<parquet::WriterProperties> props = builder.build();

        // Create a ParquetFileWriter instance
        parquet_writer = parquet::ParquetFileWriter::Open(file_writer, schema, props);

        rgw = parquet_writer->AppendBufferedRowGroup();

    } catch (std::exception &e) {
        throw Php::Exception(e.what());
    }
}


/*! \fn create_reader(Php::Parameters &params) 
    \brief The real constructor of the class ParquetWriter
    \param params The params entered by the user: must contain at least 2 values
    1st value is the parquet file name to be written into.
    2nd value is the number of row group to be read 
*/
void Parquet::create_reader(Php::Parameters &params)
{
    std::string file_name = params[0].stringValue();
    int row_group_id = params[1].numericValue();
    #ifdef DEBUG
    printf("Reading file %s\n", file_name.c_str());
    #endif
    try {
            parquet_file_reader = parquet::ParquetFileReader::OpenFile(file_name, false);

            // Get the File MetaData
            std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_file_reader->metadata();
            std::shared_ptr<parquet::RowGroupReader> row_group_reader= parquet_file_reader->RowGroup(row_group_id);

            int num_columns = parquet_file_reader->metadata()->num_columns();
            for (int i = 0; i < num_columns; i++) {
                column_readers.push_back(row_group_reader->Column(i));
            }

    } catch (const std::exception& e) {
        throw Php::Exception(e.what());
    }
}


/*! \fn void read_col_int(int col, Php::Array& ar_col).
    \brief Reads column and returns the value in an array type.
    \param col Identifier of the column to be read.
    \param ar_col The values of the column read, must be passed by reference
*/ 
void Parquet::read_col_int(int col, Php::Array& ar_col) {
    #ifdef DEBUG
    printf("Reading Col #%d\n", col);
    #endif

    try {
        std::shared_ptr<parquet::ColumnReader> column_reader = column_readers[col];
        const parquet::ColumnDescriptor* descr = column_reader->descr();
        reader_t type_reader = type_readers[column_reader->type()];            
        type_reader(column_reader, descr->max_definition_level(), descr->max_repetition_level(), ar_col);
    } catch (const std::exception& e) {
        throw Php::Exception(e.what());
    }
}


/*! \fn void read_col(Php::Parameters& params).
    \brief The wrapper function to be seen from PHP.
    \param params
    1st parameter will contain the row id.
    2nd parameter will contain the return value.
*/
void Parquet::read_col(Php::Parameters& params) {

    int col = params[0].numericValue();
    Php::Array ar_cols;
    read_col_int(col, ar_cols);

    params[1] = ar_cols;
}


/*! \fn void read_element(Php::Parameters& params)
    \brief Reads 1 single element of the column
    \param params
        1st paremeter will contain the row id
        2nd parameter will contain the column id
        3rd paremeter will contain the result of the read
*/
void Parquet::read_element(Php::Parameters& params) {

    if(params.size() != 3) {
        throw Php::Exception("Must enter 3 parameters.");
        return;
    }
    
    int row = params[0].numericValue();
    int col = params[1].numericValue();

    Php::Array ar_col;

    read_col_int(col, ar_col);

    #if DEBUG
    print_array_json(ar_col, 2);
    #endif

    const std::shared_ptr<const parquet::LogicalType> logical_type = column_readers[col]->descr()->logical_type();

    if (logical_type->is_string())
        params[2] = ar_col[row].value().stringValue();
    else if (logical_type->is_int())
        params[2] = ar_col[row].value().numericValue();
    else if (logical_type->is_decimal())
        params[2] = ar_col[row].value().floatValue();
    else
        params[2] = ar_col[row].value().stringValue();
}


/*! \fn void Write(Php::Parameters& params)
    \brief Writes the array of values inside the parquet file opened.
    \param params Array of values, Must at least have 2 values.
    First parameter should be the array of values to be inserted.
    Second parameter should be the number of rows sucessfully written in the file.
*/
void Parquet::Write(Php::Parameters& params) {


    if (!params[0].isArray()) {
        throw Php::Exception("[Parameters Error] Parameter is not an array!\n");
        return;
    }

    Php::Array input = params[0]; // ARRAY OF ROWS!!
    int num_rows = input.length();

    #ifdef DEBUG
    printf("Number of rows = %d\n", num_rows);
    #endif
    
    try {

        for (int i = 0; i < nbr_columns; i++) {
            parquet::ColumnWriter *column_writer = rgw->column(i);
            const parquet::ColumnDescriptor *descr = column_writer->descr();
            std::vector<std::string> vPath = descr->path()->ToDotVector();
            int16_t maxdef = descr->max_definition_level();
            int16_t maxrep = descr->max_repetition_level();
            int16_t zerorep = 0;
            bool is_required = descr->schema_node()->is_required();
            writer_t type_writer = type_writers[column_writer->type()];

            for (int j = 0; j < num_rows; j++) {
                
                #ifdef DEBUG
                printf("Getting Value of Path %s", descr->path()->ToDotString().c_str());
                #endif
                
                Php::Value val_from_path = get_value_from_vector_path(input[j], vPath, 0);

                // Value is it really NULL?? 
                if (val_from_path.isNull()) { // This is a map/map.key_value, so will return null
                    const parquet::schema::Node *parent_node = descr->schema_node()->parent();
                    if (parent_node->logical_type()->is_map()) {
                        std::vector<std::string> v_real_map_path;
                        // Is this a may.key_value.key
                        if (parent_node->name().compare("key_value") == 0) v_real_map_path = parent_node->parent()->path()->ToDotVector();
                        // Or directly a map.key
                        else v_real_map_path = parent_node->path()->ToDotVector();    
                        
                        Php::Value map_val_from_path = get_value_from_vector_path(input[j], v_real_map_path, 0);

                        if (map_val_from_path.isArray()) {

                            Php::Array Keys;
                            Php::Array Values;
                            
                            get_keys_values(map_val_from_path, Keys, Values);

                            // Write the keys column values
                            // Insert the keys
                            int len = Keys.length();
                            type_writer(column_writer, Keys.get(0), &maxdef, &zerorep, is_required);
                            for (int k = 1; k < len; k++) {
                                type_writer(column_writer, Keys.get(k), &maxdef, &maxrep, is_required);
                            }

                            // Move to the values column
                            // Increment i since we have moved to the next column
                            i++;

                            column_writer = rgw->column(i);
                            descr = column_writer->descr();

                            maxdef = descr->max_definition_level();
                            maxrep = descr->max_repetition_level();
                            is_required = descr->schema_node()->is_required();
                            type_writer = type_writers[column_writer->type()];

                            // Insert the values
                            type_writer(column_writer, Values.get(0), &maxdef, &zerorep, is_required);
                            for (int k = 1; k < len; k++) {
                                type_writer(column_writer, Values.get(k), &maxdef, &maxrep, is_required);
                            }
                        }
                    }  else if (parent_node->logical_type()->is_list()) {

                        std::vector<std::string> v_real_map_path = parent_node->path()->ToDotVector();    
                        
                        Php::Value map_val_from_path = get_value_from_vector_path(input[j], v_real_map_path, 0);

                        if (map_val_from_path.isArray()) {
                            auto itr = map_val_from_path.begin();
                            type_writer(column_writer, itr->second, &maxdef, &zerorep, is_required);

                            for (; itr != map_val_from_path.end(); ++itr) {
                                type_writer(column_writer, itr->second, &maxdef, &maxrep, is_required);
                            }
                        }
                    } else { // This is a NULL Value!
                        type_writer(column_writer, val_from_path, &maxdef, nullptr, is_required);
                    }              
                } else if (!val_from_path.isArray()) {
                    type_writer(column_writer, val_from_path, &maxdef, nullptr, is_required);
                } else {
                    Php::Array array = val_from_path;
                    int len = array.length();
                    type_writer(column_writer, array.get(0), &maxdef, &zerorep, is_required);
                    for (int k = 1; k < len; k++) {
                        type_writer(column_writer, array.get(k), &maxdef, &maxrep, is_required);
                    }
                }
            }
        }
            
    } catch (std::exception &e) {
        throw Php::Exception(e.what());
    }
}

/*! \fn NodePtr SetupSchema(std::string root_name, parquet::Repetition::type root_repetition, Php::Array arr_properties, parquet::ConvertedType::type converted_type = parquet::ConvertedType::type::NONE)
    \brief This function will read the schema from the user input and write it inside the parquet file
    \param root_name Name of the schema
    \param root_repetition repetition of the schema
    \param arr_properties The array that contains the key/value map of the schema
    \param converted_type contains the converted type of the schema, initialized to NONE 
*/
NodePtr Parquet::SetupSchema(std::string root_name, parquet::Repetition::type root_repetition, Php::Array schema, parquet::ConvertedType::type converted_type) {
        
    parquet::schema::NodeVector fields;
    Php::Array properties = get_keys(schema);
    int len = properties.length();
    
    for (int i = 0; i < len; i++) {
        Php::Value key = properties.get(i);
        Php::Value value = schema.get(key.stringValue().c_str());
        
        Php::Value type = value.get("type");
        Php::Value optional = value.get("optional");
        Php::Value repeat = value.get("repeat");
        Php::Value annotation = value.get("annotation");
        Php::Value name = value.get("name");

        parquet::Type::type parquet_type = parquet::Type::BOOLEAN;
        parquet::ConvertedType::type converted_type = parquet::ConvertedType::type::NONE;
        Repetition::type repetition;

        std::string type_str = std::string(type.stringValue());
        std::string key_str = std::string(key.stringValue());
        std::string annotation_str = std::string(annotation.stringValue());
        std::string name_str = std::string(name.stringValue());
        
        Node::type node_type = Node::PRIMITIVE;
        
        repetition = get_reptition_type(repeat);

        if (type_str.compare("bool") == 0) {
            parquet_type = parquet::Type::BOOLEAN;
            nbr_columns++;
        } else if (type_str.compare("int32") == 0) {
            parquet_type = parquet::Type::INT32;
            nbr_columns++;
        } else if (type_str.compare("int64") == 0) {
            parquet_type = parquet::Type::INT64;
            nbr_columns++;
        } else if (type_str.compare("timestamp") == 0) {
            parquet_type = parquet::Type::INT64;
            converted_type = parquet::ConvertedType::TIMESTAMP_MILLIS;
            nbr_columns++;
        } else if (type_str.compare("int96") == 0) {
            parquet_type = parquet::Type::INT96;
            nbr_columns++;
        } else if (type_str.compare("float") == 0) {
            parquet_type = parquet::Type::FLOAT;
            nbr_columns++;
        } else if (type_str.compare("double") == 0) {
            parquet_type = parquet::Type::DOUBLE;
            nbr_columns++;
        } else if (type_str.compare("string") == 0) {
            parquet_type = parquet::Type::BYTE_ARRAY;
            converted_type = parquet::ConvertedType::UTF8;
            nbr_columns++;
        } else if (type_str.compare("byte_array") == 0) {
            parquet_type = parquet::Type::BYTE_ARRAY;
            nbr_columns++;
        } else if (type_str.compare("fixed_len_byte_array") == 0) {
            parquet_type = parquet::Type::FIXED_LEN_BYTE_ARRAY;
            nbr_columns++;
        } else if (type_str.compare("group") == 0) {
            node_type = Node::GROUP;
        
            if (annotation_str.compare("MAP") == 0) {
                converted_type = parquet::ConvertedType::MAP;
            } else if (annotation_str.compare("MAP_KEY_VALUE") == 0) {
                converted_type = parquet::ConvertedType::MAP_KEY_VALUE;
            } else if (annotation_str.compare("LIST") == 0) {
                converted_type = parquet::ConvertedType::LIST;
            }
        } 

        if (node_type == Node::GROUP) {

            Php::Value group_schema = value.get("schema");

            if (converted_type == parquet::ConvertedType::MAP) {
                nbr_columns++;
                nbr_columns++;
                
                Php::Value map_repeat = group_schema.get("repeat").boolValue();

                Php::Value map_key_value_schema = group_schema.get("schema");
                
                // First look for the key_value
                Php::Value key_type = map_key_value_schema.get("key_type");
                Php::Value value_type = map_key_value_schema.get("value_type");
                
                parquet::schema::NodeVector map_key_value_fields;
                parquet::schema::NodeVector map_fields;

                //Make a key primitive with key_type
                map_key_value_fields.push_back(PrimitiveNode::Make("key", Repetition::REQUIRED, get_parquet_type(key_type), get_converted_type(key_type)));

                //Make a value pritimive with value_type
                if (value_type.stringValue().compare("group") == 0) map_key_value_fields.push_back(SetupSchema(key_str, repetition, group_schema, converted_type));
                else map_key_value_fields.push_back(PrimitiveNode::Make("value", repetition, get_parquet_type(value_type), get_converted_type(value_type)));

                // Insert a group node called "key_value"
                NodePtr Map_Key_Value_Node = GroupNode::Make("key_value", Repetition::REPEATED, map_key_value_fields, parquet::ConvertedType::MAP_KEY_VALUE);
                map_fields.push_back(Map_Key_Value_Node);
                
                NodePtr Map_Node = GroupNode::Make(name.stringValue().c_str(), root_repetition, map_fields, parquet::ConvertedType::MAP);
                fields.push_back(Map_Node);

            } else if (converted_type == parquet::ConvertedType::LIST) {
                nbr_columns++;
                
                Php::Value list_schema = group_schema.get("schema");
                Php::Value list_repeat = group_schema.get("repeat");
                Php::Value list_name = group_schema.get("name");
                Php::Value list_type = list_schema.get("type").stringValue();

                if (list_type.stringValue().compare("group") == 0) {
                    
                    fields.push_back(SetupSchema(key_str, repetition, group_schema, converted_type));
                    
                } else {
                    Php::Value list_element_type = list_schema.get("type");
                    
                    parquet::schema::NodeVector list_fields;
                    
                    list_fields.push_back(PrimitiveNode::Make("array", get_reptition_type(list_repeat), get_parquet_type(list_element_type), get_converted_type(list_element_type)));
                    
                    NodePtr List_Node = GroupNode::Make(name_str, get_reptition_type(repeat), list_fields, converted_type);
                    
                    fields.push_back(List_Node);
                }
            } else {
                fields.push_back(SetupSchema(key_str, repetition, group_schema, converted_type));
            }
        } else {
            fields.push_back(PrimitiveNode::Make(key_str, repetition, parquet_type, converted_type));
        }
    }
   
    return GroupNode::Make(root_name, root_repetition, fields, converted_type);
}
    

/*! \fn get_file_json(Php::Parameters& params)
    \brief Reads the parquet file and returns the content of the file in json-format 
    \param params Param is passed by reference and 1st element will contain the json-format of the parquet file
*/
void Parquet::get_file_json(Php::Parameters& params) {

    try {
        parquet::ParquetFilePrinter printer(parquet_file_reader.get());
        std::ostringstream stream;
        std::list<int> columns;
        printer.JSONPrint(stream, columns, file_name.c_str());
        Php::Value json_file = stream.str();             
        params[0] = json_file;
    } catch (const std::exception& e) {
        throw Php::Exception(e.what());
    }
}


/*! \fn static void walkSchema(const NodePtr& node, Php::Value &res)
    \brief Reads the schema of the parquet file
    \param node Node to be read
    \param res schema of the Node 
*/
void Parquet::walkSchema(const NodePtr& node, Php::Value &res) {

    Php::Object obj;

    res[node->name().c_str()] = obj;


    if (node->is_optional()) {
        obj["optional"] = node->is_optional();
    }

    if (node->is_group()) {
        const GroupNode* group = static_cast<const GroupNode*>(node.get());
        for (int i = 0, len = group->field_count(); i < len; i++) {
            walkSchema(group->field(i), obj);
        }
        return;
    }

    const PrimitiveNode* primitive = static_cast<const PrimitiveNode*>(node.get());
    switch (primitive->physical_type()) {
        case parquet::Type::BOOLEAN:
            obj["type"] = "BOOL";
            break;
        case parquet::Type::INT32:
            obj["type"] = "INT32";
            break;
        case parquet::Type::INT64:               
            if (node->logical_type()->is_timestamp()) {
                obj["type"] = "TIMESTAMP";
            } else {
                obj["type"] = "INT64";
            }
            break;
        case parquet::Type::INT96:
            obj["type"] = "INT96";
            break;
        case parquet::Type::FLOAT:
            obj["type"] = "FLOAT";
            break;
        case parquet::Type::DOUBLE:
            obj["type"] = "DOUBLE";
            break;
        case parquet::Type::BYTE_ARRAY:
            if (node->logical_type()->is_string()) {
                obj["type"] = "STRING";
            } else {
                obj["type"] = "BYTE ARRAY";
            }            
            break;
        case parquet::Type::FIXED_LEN_BYTE_ARRAY:
            obj["type"] = "FIXED LENGTH BYTE ARRAY";
            break;
        case parquet::Type::UNDEFINED:
            obj["type"] = "UNDEFINED";
            break;
    }
}


/*! \fn Php::Value getInfo() 
    \brief Extracts the info about the parquet file: version, number of rows groups, number of columns, number of rows and created by
*/
Php::Value Parquet::getInfo() 
{

    std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_file_reader->metadata();
    
    Php::Value Info;
    const NodePtr root = file_metadata->schema()->schema_root();

    Info["version"] = file_metadata->version();
    Info["num_row_groups"] = file_metadata->num_row_groups();
    Info["num_columns"] = file_metadata->num_columns();
    Info["num_rows"] = file_metadata->num_rows();
    Info["created_by"] = file_metadata->created_by();
    
    Php::Array schema;

    walkSchema(root, schema);
    
    Info["schema"] = schema;

    return Info;
}


/*! \fn void close_writer() 
    \brief Closes the parquet writer 
*/ 
void Parquet::close_writer()
{
    try {
        parquet_writer->Close();
    } catch (std::exception& e) {
        throw Php::Exception(e.what());
    }
}


/*! \fn void close_reader()
    \brief Closes the parquet reader 
*/
void Parquet::close_reader()
{
    try {
        parquet_file_reader->Close();
    } catch (std::exception& e) {
        throw Php::Exception(e.what());
    }
}

