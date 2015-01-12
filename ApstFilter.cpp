/* Copyright (c) 2005 - 2012 Vertica, an HP company -*- C++ -*- */

#include "Vertica.h"
#include <string>
#include <fstream>
using namespace Vertica;
using namespace std;

/**
 * GZipUnpacker. Decodes .gz format.
 */
class ApstFilter : public UDFilter {
private:
	#define APST '"'
	#define ESC '`'

	bool in_str;
	//ofstream debug_file;

	bool at_n_apst(const char* line, int index, int num, int length) {
		if (index < 0 || index >= length)
			throw 1;
		if (num <= 0)
			throw 2;
		if (index+num-1>=length)
			throw 3;

		for (; num > 0; num--,index++) {
			if (line[index] != APST)
				return false;
		}
		return true;
	}

protected:
	virtual void setup(ServerInterface &srvInterface) {
		this->in_str = false;
		//this->debug_file.open("/home/dbadmin/vertica_filter/debug_file.csv", ios_base::out | ios_base::trunc);
	}

	virtual StreamState process(ServerInterface &srvInterface,
				  DataBuffer      &input,
				  InputState       input_state,
				  DataBuffer      &output)
	{
		char* input_line = input.buf + input.offset;
		char* output_line = output.buf + output.offset;
		size_t input_length = input.size - input.offset;
		size_t output_length = output.size - output.offset;
		size_t length = input_length < output_length ? input_length : output_length;

		/*
 		*  in case we haven't got the last byte of the file yet,
 		*  we want the last character in the buffer to be != '"'
 		*  otherwise we can't know if it's a single '"' or some other continuous series of '"'
 		*  because we don't have the character that follows this '"'
 		*  and since we treat each length of series of '"' differently
 		*  we must know the exact length of this series
 		*  if all characters in buffer are '"', we ask for additional data
		*/

		//srvInterface.log("input_length is %zu, output_length is %zu\n", input_length, output_length);

		//srvInterface.log("input_state != END_OF_FILE : %d\n", input_state != END_OF_FILE);
		if (input_state != END_OF_FILE || length < input_length) {
			while (length > 2 && input_line[length-1] == '"')
				length--;
			if (length <= 2) {
				if (input_length < output_length) {
					//srvInterface.log("requesting for more input\n");
					return INPUT_NEEDED;
				}
				else {
					//srvInterface.log("requesting for more output\n");
					return OUTPUT_NEEDED;
				}
			}
		}
		//srvInterface.log("actual length is %zu\n", length);
		
		strncpy(output_line, input_line, length);

		size_t i;
		for (i=0; i<length-2; i++) {
			try {
				if (output_line[i] == '\n') {
					// reset the in_str to false (since a new row is coming)
					// just in case there was some error in the csv and some
					// string in the current line was not properly terminated
					this->in_str = false;
					continue;
				}
				if (this->at_n_apst(output_line, i, 3, length)) {
					//srvInterface.log("found 3 '\"' in index %zu\n", i);
					if (this->in_str) {
						// "...."""
						output_line[i] = ESC;
					} else {
						// """...."
						output_line[i+1] = ESC;
					}
					i++;
					continue;
				}
				if (this->at_n_apst(output_line, i, 2, length)) {
					//srvInterface.log("found 2 '\"' in index %zu, in_str=%d", i, this->in_str);
					if (this->in_str)
						output_line[i] = ESC;
					i++;
					continue;
				}
				if (output_line[i] == APST) {
					//srvInterface.log("found '\"' in index %zu, switching in_str to %d\n", i, !this->in_str);
					this->in_str = !this->in_str;
				}
			} catch (int exception_num) {
				switch (exception_num) {
				case 1:
					vt_report_error(1, "got index %zu and length %zu", i, length);
					break;
				case 2:
					vt_report_error(2, "got num of '%c's <= 0", ESC);
					break;
				case 3:
					vt_report_error(3, "got index+num-1>=length. index is %zu, length is %zu", i, length);
					break;
				default:
					vt_report_error(10, "unknown error occured");
				}
			}
		}

		if (output_line[i] == '"') {
			// output_line[i]=='"' -> i<length-1 (last char is never '"') -> i<=length-2
			// length > 2 and i >= length-2 (loop condition) -> i >= length-2 > 0 -> i>0 -> i-1 >= 0
			if (output_line[i-1] != ESC) {
				//srvInterface.log("found '\"' in index %zu, switching in_str to %d\n", i, !this->in_str);
				this->in_str = !this->in_str;
			}
		}
		
		/*
		char* output_debug = new char[length+1];
		strncpy(output_debug, output_line, length);
		output_debug[length] = 0;
		this->debug_file << output_debug;
		this->debug_file.flush();
		delete output_debug;
		*/

		//srvInterface.log("input.offset is %zu, output.offset is %zu\n", input.offset, output.offset);

		input.offset += length;
		output.offset += length;
		
		//srvInterface.log("input.offset is %zu, output.offset is %zu\n", input.offset, output.offset);
		//srvInterface.log("input.size is %zu, output.size is %zu\n", input.size, output.size);

		//srvInterface.log("input_state == END_OF_FILE : %d\n", input_state == END_OF_FILE);

		if (input_state == END_OF_FILE && input.offset == input.size)
			return DONE;
		else {
			if (output.offset == output.size) {
				//srvInterface.log("requesting for more output\n");
				return OUTPUT_NEEDED;
			}
			else {
				//srvInterface.log("requesting for more input\n");
				return INPUT_NEEDED;
			}
		}
	}

    virtual void destroy(ServerInterface &srvInterface) {
	//this->debug_file.close();
    }

public:
	ApstFilter() {}
};




class ApstFilterFactory : public FilterFactory {
public:
    virtual void plan(ServerInterface &srvInterface,
            PlanContext &planCtxt)
    { /* No plan-time setup work to do */ }

    virtual UDFilter* prepare(ServerInterface &srvInterface,
            PlanContext &planCtxt)
    {
        return vt_createFuncObj(srvInterface.allocator, ApstFilter);
    }
};
RegisterFactory(ApstFilterFactory);
