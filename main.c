#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "schema.h"
#include "btree.h"


typedef struct  InputBuffer {
    char* buffer;
    size_t buffer_length;
    size_t input_length;
} InputBuffer;

typedef struct CommandResult {

};

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
    UserTable* schema_table;
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

 ProcessStatementResultType process_command(InputBuffer *input_buffer) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        exit(EXIT_SUCCESS);
    }else {
        return STATEMENT_UN_RECOGNISED;
    }
}

SchemaType get_table(InputBuffer* input_buffer) {
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

PreparedStatementResult prepare_statement(InputBuffer *input_buffer) {
    PreparedStatementResult result;
    result.result_type = PREPARE_UN_RECOGNISED;
    if (strncmp(input_buffer->buffer, "INSERT", 6)==0 || strncmp(input_buffer->buffer, "insert", 6) == 0) {
        result.statementType = INSERT;
        result.result_type = PREPARE_SUCESS;
    }

    if (strncmp(input_buffer->buffer, "SELECT", 6)==0 || strncmp(input_buffer->buffer, "select", 6) == 0) {
        result.statementType = SELECT;
        result.result_type = PREPARE_SUCESS;
    }

    SchemaType schema = get_table(input_buffer);
    switch (schema) {
        case USER_TABLE:
            result.schema_type = USER_TABLE;
            UserTable* user_table = malloc(sizeof(UserTable));
            int args_assigned =
                sscanf(input_buffer->buffer, "insert into user %d %s", &(user_table->id), user_table->name);
            if (args_assigned < 2) {
                result.result_type = PREPARE_SYNTAX_ERROR;
            }
            result.schema_table = user_table;
            break;
    }
    return result;
}

void execute_statement(InputBuffer* input_buffer, StatementType statement_type) {
    switch (statement_type) {
        case INSERT:
            printf("Insert into Table \n");
            break;
        case SELECT:
            printf("Select from Table \n");
            break;
    }
}

int main(void){
    InputBuffer* input_buffer = createBuffer();
    while(true) {
        print_promt();
        read_input(input_buffer);
        if (input_buffer->buffer[0]=='.') {
            ProcessStatementResultType result = process_command(input_buffer);
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
        execute_statement(input_buffer, prepared_statement_result.statementType);
    }
    return 0;
}
