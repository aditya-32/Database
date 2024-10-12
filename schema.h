#define size_of_attr(Struct, Attribute ) sizeof(((Struct *)0)->Attribute)
#define MAX_NAME_LENGTH 255
#define PAGE_SIZE 4096
#define TABLE_MAX_PAGES 100

typedef struct Row {
    char name[MAX_NAME_LENGTH];
    int id;
} Row;


typedef enum SchemaType {
    USER_TABLE,
    INVALID
}SchemaType;

typedef union  Schema {
    Row* userTable;
}Schema;


typedef struct {
    int file_descriptor;
    int file_length;
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    uint32_t num_rows;
    Pager* pager;
} Table;

const uint32_t ID_OFFSET = 0;
const uint32_t ID_SIZE = size_of_attr(Row, id);
const uint32_t NAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t NAME_SIZE = size_of_attr(Row, name);
const uint32_t ROW_SIZE = ID_SIZE + NAME_SIZE;
const uint32_t ROW_PER_PAGE = PAGE_SIZE/ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = TABLE_MAX_PAGES * ROW_PER_PAGE;

