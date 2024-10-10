// bptree.h
#ifndef BPTREE_H
#define BPTREE_H

#define MAX_KEYS 3  // Example order of the B+ tree
#define ORDER MAX_KEYS+1  // Example order of the B+ tree

// Structure for the B+ tree node
typedef struct Node {
    int is_leaf;
    int num_keys;
    int keys[MAX_KEYS];
    struct Node *children[ORDER];
    struct Node *next;
} Node;

typedef struct BPlusTree {
    Node* root; // Root node of the B+ tree
} BPlusTree;


// Function prototypes
Node* createNode(int is_leaf);
void insert(BPlusTree *root, int key);
void print_tree(Node *root, int level);
BPlusTree *createBPlusTree();
#endif