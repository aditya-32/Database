#include <stdio.h>
#include "btree.h"

int main(void){
    BPlusTree* tree = createBPlusTree();

    // Insert keys into the B+ tree
    for (int i = 1; i <= 10; i++) {
        insert(tree, i);
    }

    // Print the B+ tree
    print_tree(tree->root, 0);
    return 0;
}
