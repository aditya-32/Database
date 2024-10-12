#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "schema.h"
#include <unistd.h>
#include <fcntl.h>
#include "btree.h"


typedef struct  InputBuffer {
    char* buffer;
    size_t buffer_length;
    size_t input_length;
} InputBuffer;

typedef enum {
    EXECUTE_SUCCESS,
    TABLE_FULL
} ExecuteResult;


typedef enum ProcessStatementResultType {
    STATEMENT_RECOGNISED,
    STATEMENT_UN_RECOGNISED,
} ProcessStatementResultType;


typedef enum PreparedStatementResultType {
    PREPARE_SUCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UN_RECOGNISED,
} PreparedStatementResultType;


typedef enum StatementType {
    INSERT,
    SELECT,
} StatementType;

typedef struct PreparedStatementResult {
    Row* row;
    SchemaType schema_type;
    StatementType statementType;
    PreparedStatementResultType result_type;
}PreparedStatementResult;


InputBuffer* createBuffer() {
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length= 0;
    return input_buffer;
}

void print_promt(void) {
    printf("adb> ");
}

void read_input(InputBuffer* input_buffer) {
    ssize_t bytes_read = getline(&input_buffer->buffer, &input_buffer->buffer_length, stdin);
    if (bytes_read < 0) {
        printf("Error while reading input");
        exit(EXIT_FAILURE);
    }
    input_buffer->buffer[bytes_read-1] = 0;
    input_buffer->input_length = bytes_read-1;
}

void close_input_buffer(InputBuffer *input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

void page_flush(Pager *pager, uint32_t page_num, int size) {
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush NULL page");
        exit(EXIT_FAILURE);
    }
    off_t offset = (pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        printf("Error while Seeking Page %d", page_num);
        exit(EXIT_FAILURE);
    }
    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);
    if (bytes_written == 1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void db_close(Table *table) {
    Pager* pager = table->pager;
    uint32_t complete_pages = table->num_rows / ROW_PER_PAGE;
    for (int i=0;i<complete_pages;i++) {
        if (pager->pages[i] != NULL) {
            page_flush(pager, i, PAGE_SIZE);
            free(pager->pages[i]);
            pager->pages[i] = NULL;
        }
    }
    uint32_t additional_rows = table->num_rows % ROW_PER_PAGE;
    if (additional_rows > 0) {
        if (pager->pages[complete_pages] != NULL) {
            page_flush(pager, complete_pages, PAGE_SIZE);
            free(pager->pages[complete_pages]);
            pager->pages[complete_pages] = NULL;
        }
    }
    int result = close(pager->file_descriptor);
    if(result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for (int i=0;i<TABLE_MAX_PAGES;i++) {
        void *page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

 ProcessStatementResultType process_command(InputBuffer *input_buffer, Table *table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        close_input_buffer(input_buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
    }else {
        return STATEMENT_UN_RECOGNISED;
    }
}

SchemaType get_select_table(InputBuffer* input_buffer) {
    char* ptr = strstr(input_buffer->buffer, "* from");
    while(*ptr != ' ') {
        ptr++;
    }
    *ptr++;
    int len=0;
    char table_name[50];
    while(*ptr != ' ' && *ptr != '(' && *ptr != '\0') {
        table_name[len++] = *ptr;
        *ptr++;
    }
    if (strncmp(table_name, "user", len) == 0) {
        return USER_TABLE;
    }
    return INVALID;
}

SchemaType get_insert_table(InputBuffer* input_buffer) {
    char* ptr = strstr(input_buffer->buffer, "into");
    while(*ptr != ' ') {
        ptr++;
    }
    *ptr++;
    int len=0;
    char table_name[50];
    while(*ptr != ' ' && *ptr != '(' && *ptr != '\0') {
        table_name[len++] = *ptr;
        *ptr++;
    }
    if (strncmp(table_name, "user", len) == 0) {
        return USER_TABLE;
    }
    return INVALID;
}

void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Trying to get page out of bounds\n");
        exit(EXIT_FAILURE);
    }
    if (pager->pages[page_num] == NULL) {
        void *page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length % PAGE_SIZE;
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }
        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t byte_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (byte_read < 0) {
                printf("Error while reading page");
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}


void *row_slot(Table *table, uint32_t row_num) {
    uint32_t page_num = row_num / ROW_PER_PAGE;
    void* page = get_page(table->pager, page_num);

    uint32_t row_offset = row_num % ROW_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}

void enrich_row_data(InputBuffer* input_buffer, PreparedStatementResult* result) {
    SchemaType schema = get_insert_table(input_buffer);
    switch (schema) {
        case USER_TABLE:
            result->schema_type = USER_TABLE;
        Row* user_table = malloc(sizeof(Row));
        int args_assigned =
            sscanf(input_buffer->buffer, "insert into user %d %s", &(user_table->id), user_table->name);
        if (args_assigned < 2) {
            result->result_type = PREPARE_SYNTAX_ERROR;
        }
        result->row = user_table;
        break;
        default: printf("Unknown table type\n");
    }
}

PreparedStatementResult prepare_statement(InputBuffer *input_buffer) {
    PreparedStatementResult result;
    result.result_type = PREPARE_UN_RECOGNISED;
    if (strncmp(input_buffer->buffer, "INSERT", 6)==0 || strncmp(input_buffer->buffer, "insert", 6) == 0) {
        result.statementType = INSERT;
        result.result_type = PREPARE_SUCESS;
        enrich_row_data(input_buffer, &result);
    }

    if (strncmp(input_buffer->buffer, "SELECT", 6)==0 || strncmp(input_buffer->buffer, "select", 6) == 0) {
        result.statementType = SELECT;
        result.result_type = PREPARE_SUCESS;
    }
    return result;
}

void serialise_row(Row *source, void *destination) {
    memcpy(destination+ID_OFFSET, &source->id, ID_SIZE);
    memcpy(destination+NAME_OFFSET, source->name, NAME_SIZE);
}

void deserialise_row(void* source, Row *destination) {
    memcpy(&destination->id, source+ID_OFFSET, ID_SIZE);
    memcpy(&destination->name, source+ID_OFFSET+ID_SIZE, NAME_SIZE);
}

ExecuteResult execute_insert(Row *row, Table* table) {
    if (table->num_rows == TABLE_MAX_ROWS) {
        return TABLE_FULL;
    }
    serialise_row(row, row_slot(table, table->num_rows));
    table->num_rows++;
    return EXECUTE_SUCCESS;
}

void print_row(Row* row) {
    printf("(%d, %s)\n", row->id, row->name);
}

ExecuteResult execute_select(Row *row, Table* table) {
    for (uint32_t i = 0; i < table->num_rows; i++) {
        deserialise_row(row_slot(table, i), row);
        print_row(row);
    }
    return EXECUTE_SUCCESS;
}

Pager* open_pager(const char* filename) {
    Pager* pager = malloc(sizeof(Pager));
    int fd = open(filename,
                  O_RDWR | // Read/Write mode
                  O_CREAT, // Create file if it does not exist
                  S_IWUSR | // User write permission
                  S_IRUSR   // User read permission
    );
    if (fd == 1) {
        printf("Failed to open pager\n");
        exit(EXIT_FAILURE);
    }
    pager->file_length = lseek(fd, 0, SEEK_END);
    pager->file_descriptor = fd;
    for (int i=0; i<TABLE_MAX_ROWS; i++) {
        pager->pages[i] = NULL;
    }
    return pager;
}

Table *db_open(const char* filename) {
    Pager* pager = open_pager(filename);
    Table *table = malloc(sizeof(Table));
    table->num_rows = 0;
    table->pager = pager;
    return table;
}

ExecuteResult execute_statement(Row *row, StatementType statement_type, Table *table) {
    switch (statement_type) {
        case INSERT:
            return execute_insert(row, table);
        case SELECT:
            return execute_select(row, table);
    }
}

int main(int argc, char* argv[] ){
    InputBuffer* input_buffer = createBuffer();
    if (argc < 2) {
        printf("Must provide the database name");
        exit(EXIT_FAILURE);
    }
    char *filename = argv[1];
    Table *table = db_open(filename);
    while(true) {
        print_promt();
        read_input(input_buffer);
        if (input_buffer->buffer[0]=='.') {
            ProcessStatementResultType result = process_command(input_buffer, table);
            switch (result) {
                case STATEMENT_UN_RECOGNISED:
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
                case STATEMENT_RECOGNISED:;
            }
        }

        PreparedStatementResult prepared_statement_result = prepare_statement(input_buffer);
        switch (prepared_statement_result.result_type) {
            case PREPARE_SUCESS: break;
            case PREPARE_SYNTAX_ERROR:
                printf("invalid command '%s'\n", input_buffer->buffer);
                continue;
            case PREPARE_UN_RECOGNISED:
                printf("Unrecognized command '%s'\n", input_buffer->buffer);
                continue;
        }
        ExecuteResult result = execute_statement(prepared_statement_result.row, prepared_statement_result.statementType, table);
        switch (result) {
            case EXECUTE_SUCCESS:
                printf("Command Executed Successfully\n");
            break;
            case TABLE_FULL:
                printf("Table Full\n");
            break;
        }
    }
    return 0;
}
