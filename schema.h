#define MAX_NAME_LENGTH 255

typedef struct UserTable {
    char name[MAX_NAME_LENGTH];
    int id;
} UserTable;


typedef enum SchemaType {
    USER_TABLE,
    INVALID
}SchemaType;

typedef union  Schema {
    UserTable* userTable;
}Schema;