#define _GNU_SOURCE
#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<stdbool.h>
#include<stdint.h>
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

#define COLUMN_USERNAME_SIZE  32
#define COLUMN_EMAIL_SIZE  255


struct Row_t {
	uint32_t id;
	char username[COLUMN_USERNAME_SIZE + 1];
	char email[COLUMN_EMAIL_SIZE + 1];
};
typedef struct Row_t Row;

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
 uint32_t ID_OFFSET = 0;
 uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
 uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
 uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

 uint32_t PAGE_SIZE = 4096;
 #define TABLE_MAX_PAGES  100
 uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
 uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;


struct Pager_t {
	int file_descriptor;
	uint32_t file_length;
	void* pages[TABLE_MAX_PAGES];
};
typedef struct Pager_t Pager;

struct Table_t {
	Pager* pager;
	uint32_t num_rows;
};
typedef struct Table_t Table;

void serialize_row(Row* source, void* destination) {
	memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
	memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
	memcpy(destination + EMAIL_OFFSET,&(source->email), EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination) {
	memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
	memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
	memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void* row_slot(Table* table, uint32_t row_num) {
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void* page = get_page(table->pager, page_num);
	uint32_t row_offset = row_num % ROWS_PER_PAGE;
	uint32_t byte_offset = row_offset * ROW_SIZE;
	return page + byte_offset;
}

void* get_page(Pager* pager, uint32_t page_num) {
	if(page_num > TABLE_MAX_PAGES) {
		printf("Tried to fetch page number out of bounds. %d",TABLE_MAX_PAGES);
		exit(EXIT_FAILURE);
	}

	if(pager->pages[page_num] == NULL) {
		//cache miss
		void* page = malloc(PAGE_SIZE);
		uint32_t num_pages = pager->file_length / PAGE_SIZE;

		if(pager->file_length % PAGE_SIZE) {
			num_pages += 1;
		}

	  if (page_num <= num_pages) {
     	 lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      	 ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
     	 if (bytes_read == -1) {
        	printf("Error reading file: %d\n", errno);
        	exit(EXIT_FAILURE);
        }
    }
    pager->pages[page_num] = page;
  }
  return pager->pages[page_num];
}


enum MetaCommandResult_t {
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
};
typedef enum MetaCommandResult_t MetaCommandResult;

enum PrepareResult_t {
	PREPARE_SUCCESS,
	PREPARE_SYNTAX_ERROR,
	PREPARE_STRING_TOO_LONG,
	PREPARE_NEGATIVE_ID,
	PREPARE_UNRECOGNIZED_STATEMENT
};
typedef enum PrepareResult_t PrepareResult;

enum StatementType_t {
	STATEMENT_INSERT, 
	STATEMENT_SELECT
};
typedef enum StatementType_t StatementType;

enum ExecuteResult_t {
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL 
};
typedef enum ExecuteResult_t ExecuteResult;

struct Statement_t {
	StatementType type;
	Row row_to_insert;
};
typedef struct Statement_t Statement;

struct InputBuffer_t {
	char* buffer;
	size_t buffer_length;
	ssize_t input_length;
};
typedef struct InputBuffer_t InputBuffer;

InputBuffer* new_input_buffer() {
	InputBuffer* input_buffer = (InputBuffer*) malloc(sizeof(InputBuffer));
	input_buffer->buffer = NULL;
	input_buffer->buffer_length = 0;
	input_buffer->input_length = 0;
	
	return input_buffer;
}

void print_prompt() {
	printf("db_st> ");
}
void read_input(InputBuffer* input_buffer) {
	ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
	
	if(bytes_read <= 0) {
		printf("Error reading input\n");
		exit(EXIT_FAILURE);
	}
	
	input_buffer->input_length = bytes_read - 1;
	input_buffer->buffer[bytes_read - 1] = 0;
}
MetaCommandResult do_meta_command(InputBuffer* input_buffer) {
	if(strcmp(input_buffer->buffer, ".exit") == 0) {
		exit(EXIT_SUCCESS);
	} else {
		return META_COMMAND_UNRECOGNIZED_COMMAND;
	}
}

PrepareResult prepare_insert(InputBuffer* input_buffer,Statement* statement) {
	statement->type = STATEMENT_INSERT;

	char* keyword = strtok(input_buffer->buffer," ");
	char* id_string = strtok(NULL, " ");
	char* username = strtok(NULL, " ");
	char* email = strtok(NULL, " ");

	if(id_string == NULL || username == NULL || email ==  NULL) {
		return PREPARE_SYNTAX_ERROR;
	}

	int id = atoi(id_string);
	if(id < 0) {
		return PREPARE_NEGATIVE_ID;
	}
	if(strlen(username) > COLUMN_USERNAME_SIZE) {
		return PREPARE_STRING_TOO_LONG;
	}
	if(strlen(email) > COLUMN_EMAIL_SIZE) {
		return PREPARE_STRING_TOO_LONG;
	}
	statement->row_to_insert.id = id;
	strcpy(statement->row_to_insert.username, username);
	strcpy(statement->row_to_insert.email, email);

	return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
	if(strncmp(input_buffer->buffer, "insert",6) == 0) {
		return prepare_insert(input_buffer, statement);
	}

}

ExecuteResult execute_insert(Statement* statement,Table* table) {
	if(table->num_rows >= TABLE_MAX_ROWS) {
		return EXECUTE_TABLE_FULL;
	}

	Row* row_to_insert = &(statement->row_to_insert);

	serialize_row(row_to_insert,row_slot(table, table->num_rows));
	table->num_rows += 1;

	return EXECUTE_SUCCESS;
}

void print_row(Row* row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

ExecuteResult execute_select(Statement* statement, Table* table) {
	Row row;
	for(uint32_t i = 0;i < table->num_rows;i++) {
		deserialize_row(row_slot(table, i),&row);
		print_row(&row);
	}
	return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement,Table* table) {
	switch(statement->type) {
		case (STATEMENT_INSERT): 
			return execute_insert(statement, table);

		case (STATEMENT_SELECT):
			return execute_select(statement, table);
	}
}


Table* db_open(const char* filename) {
	Pager* pager = pager_open(filename);
	uint32_t num_rows = pager->file_length / ROW_SIZE;
	Table* table = (Table*) malloc(sizeof(Table));
	table->pager = pager;
	table->num_rows = num_rows;

	return table;
}

Pager* pager_open(const char* filename) {
	int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
	if(fd == -1) {
		printf("Unable to open file\n");
		exit(EXIT_FAILURE);
	}
	off_t file_length = lseek(fd, 0, SEEK_END);

	Pager* pager = malloc(sizeof(Pager));
	pager->file_descriptor = fd;
	pager->file_length = file_length;

	for(uint32_t i = 0;i < TABLE_MAX_PAGES;i++) {
		pager->pages[i] = NULL;
	}
	return pager;
}

int main(int argc, char* argv[]) {
	Table* table = new_table();
	InputBuffer* input_buffer = new_input_buffer();
	while(true) {
		print_prompt();
		read_input(input_buffer);

		if(strcmp(input_buffer->buffer, ".exit") == 0) {
			exit(EXIT_SUCCESS);
		} else {
			
			if(input_buffer->buffer[0] == '.') {
				switch(do_meta_command(input_buffer)) {
					case (META_COMMAND_SUCCESS): continue;
					case (META_COMMAND_UNRECOGNIZED_COMMAND):
						printf("Unrecognized command '%s' \n",input_buffer->buffer);
						continue;
				}
			}

			Statement statement;
			switch (prepare_statement(input_buffer, &statement)) {
				case (PREPARE_SUCCESS):
					break;
				case (PREPARE_SYNTAX_ERROR):
					printf("Syntax Error. Could not parse statement.\n");
					continue;
				case (PREPARE_STRING_TOO_LONG):
   					printf("String is too long.\n");
   					continue;
   				case (PREPARE_NEGATIVE_ID):
 			        printf("ID must be positive.\n");
			        continue;
				case (PREPARE_UNRECOGNIZED_STATEMENT): 
					printf("Unrecognized keyword at start of '%s' .\n", input_buffer->buffer);
					continue;
			}
			switch(execute_statement(&statement, table)) {
				case (EXECUTE_SUCCESS):
					printf("Executed\n");
					break;
				case (EXECUTE_TABLE_FULL):
					printf("Error: Table Full.\n");
					break;
			}

			
		}
	}
}