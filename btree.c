//
// Created by Aditya Ranjan Barik on 10/10/24.
//
#include <stdio.h>
#include <stdlib.h>
#include "btree.h"


Node *createNode (int is_leaf) {
    Node *node = malloc(sizeof(Node));
    node->is_leaf = is_leaf;
    node->num_keys = 0;
    for (int i=0; i<ORDER; i++) {
        node->children[i] = NULL;
    }
    return node;
}

Node *split_node(Node *node) {
    int key_count = (MAX_KEYS+1) / 2;
    Node *new_node = createNode(node->is_leaf);
    new_node->num_keys = node->num_keys - key_count;
    for (int i=0;i<key_count;i++) {
        new_node->keys[i] = node->keys[i+key_count];
        node->keys[i+key_count] = 0;
    }
    if (!node->is_leaf) {
        for (int i=0;i<key_count;i++) {
            new_node->children[i+1] = node->children[i+key_count+1];
            node->children[i+key_count+1] = NULL;
        }
    }
    node->num_keys = key_count;
    if (node->is_leaf) {
        new_node->next = node->next;
        node->next = new_node;
    }
    return new_node;
}

void insert_leaf(Node *node , int key) {
    int i;
    for (i = node->num_keys - 1; i >= 0 && node->keys[i] > key; i--) {
        node->keys[i + 1] = node->keys[i];
    }
    node->keys[i + 1] = key;
    node->num_keys++;
}

void split_parent_at_index(Node *parent, int index) {
    int key_count = (MAX_KEYS+1) / 2;
    // take the child node
    Node *node = parent->children[index];
    int mid_key = node->keys[key_count];
    Node *new_node = split_node(node);
    for (int j=parent->num_keys; j>index; j--) {
        parent->children[j+1] = parent->children[j];
    }
    parent->children[index+1] = new_node;
    parent->keys[index] = mid_key;
    parent->num_keys++;
}

Node *insert_non_full(Node *node, int key) {
    if (node->is_leaf) {
        insert_leaf(node, key);
    }else {
        int i = node->num_keys - 1;
        while(i>=0 && node->keys[i] > key) {
            i--;
        }
        i++;
        if (node->children[i]->num_keys == MAX_KEYS) {
            // Child is full, split it
            split_parent_at_index(node, i);
            // Determine which of the two children to insert into
            if (node->keys[i] < key) {
                i++;
            }
        }
        insert_non_full(node->children[i], key);
    }
}

Node *search(Node *node, int key) {
    Node *temp = node;
    while(!temp->is_leaf) {
        int i=0;
        while(i < temp->num_keys && temp->keys[i] <= key) {
            i++;
        }
        temp  = temp->children[i];
    }
    return temp;
}

void insert(BPlusTree* tree, int key) {
    Node * root = tree->root;
    if (root->num_keys == MAX_KEYS) {
        Node * newRoot = createNode(0);
        newRoot->children[0] = root;
        split_parent_at_index(newRoot, 0);
        tree->root = newRoot;
        insert_non_full(newRoot, key);
    }
    else {
        insert_non_full(root, key);
    }
}


void print_tree(Node *node, int level) {
    if (node == NULL) return;

    printf("Level %d: ", level);
    for (int i = 0; i < node->num_keys; i++) {
        printf("%d ", node->keys[i]);
    }
    printf("\n");

    if (!node->is_leaf) {
        for (int i = 0; i <= node->num_keys; i++) {
            print_tree(node->children[i], level + 1);
        }
    }
}

BPlusTree* createBPlusTree() {
    BPlusTree* tree = malloc(sizeof(BPlusTree));
    tree->root = createNode(1); // Start with an empty leaf node
    return tree;
}