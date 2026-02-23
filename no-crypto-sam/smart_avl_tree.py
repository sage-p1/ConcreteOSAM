from single_access_machine import read, write, alloc, return_available_address
from collections import deque
from hashlib import sha3_256
import random
import unittest

class SmartAVLNode:
    def __init__(self, hash_key=None, value=None, children=[None, None], height=1, left_height=0, right_height=0, balance=0, left_balance=None, right_balance=None):
        self.hash_key = hash_key 
        self.value = value
        self.children = children # OSAM addresses of children
        self.height = height
        self.left_height = left_height
        self.right_height = right_height
        self.balance = balance
        self.left_balance = left_balance
        self.right_balance = right_balance





class SmartAVLTree:
    def __init__(self, original_elements=None, hash_enabled=True, avl_tree_osam=True):
        self.root_address = None
        self.hash_enabled = hash_enabled # toggle hashing
        self.avl_tree_osam = avl_tree_osam
        self.structure = "SmartAVLTree"

        if original_elements is not None:
            self.build_tree(original_elements=original_elements)

    def build_tree(self, original_elements):
        if len(original_elements) <= 0:
            return

        if not self.root_address: # ensure tree is being built from scratch
            elements = []

            if type(original_elements) in [list, tuple, set]:
                # convert dictionary to be successfully processed
                og_dict = dict()
                for element in original_elements:
                    og_dict[element] = element
            else:
                og_dict = original_elements

            for key, value in og_dict.items(): 
                # hash the string representation of each key
                if self.hash_enabled:
                    hash_key = sha3_256(str(key).encode("utf-8")).digest()
                else:
                    hash_key = key
                elements.append((hash_key, value))

            # sort by digest
            elements.sort(key=lambda map_item: map_item[0])
            
            # assign new root address
            self.root_address = alloc(self.avl_tree_osam, self.structure)

            # assign indices
            left = 0
            right = len(elements) - 1

            # recursively build tree
            self._build_tree(elements, left, right, self.root_address)

        else: # adding to a built tree
            for key, value in original_elements.items():
                self.insert(key, value)
        
    def _build_tree(self, elements, left, right, address):
        # if bounds conflict
        if left > right: 
            return 0
        
        middle = (left + right) // 2 
        hash_key = elements[middle][0]
        value = elements[middle][1]
        children = []

        # determine if there are left children to add
        left_list = elements[left:middle]
        if left_list: # if the slice contains elements, there are more nodes to make
            children.append(alloc(self.avl_tree_osam, self.structure))
            left_height, left_balance = self._build_tree(elements, left, middle-1, children[0])
        else:
            children.append(None)
            left_height = 0
            left_balance = None

        # determine if there are right children to add
        right_list = elements[middle+1:right+1]
        if right_list:
            children.append(alloc(self.avl_tree_osam, self.structure))
            right_height, right_balance = self._build_tree(elements, middle+1, right, children[1])

        else:
            children.append(None)
            right_height = 0
            right_balance = None
        
        # create and write node
        height = 1 + max(left_height, right_height)
        balance = left_height - right_height
        node = SmartAVLNode(hash_key=hash_key, value=value, children=children, height=height, left_height=left_height, right_height=right_height, balance=balance, left_balance=left_balance, right_balance=right_balance)
        write(address, node, self.structure)
        return height, balance

    def __contains__(self, key):
        return self.search(key) is not None

    def search(self, key):
        return self.__getitem__(key)

    def __getitem__(self, key):
        value = None
        result = self._get_item(key)
        if result is not None:
            value = result[1].value
            write(result[0], result[1], self.structure) # write back final node
        return value

    def _get_item(self, key):
        # cannot retrieve if tree does not exist
        if self.root_address is None:
            return 

        addresses = deque()

        if self.hash_enabled:
            hash_key = sha3_256(str(key).encode("utf-8")).digest()
        else:
            hash_key = key

        current_node = read(self.root_address, self.structure) # read root (n1 <- Read(a1))
        return_available_address(self.root_address) # return root address to available addresses
        self.root_address = alloc(self.avl_tree_osam, self.structure) # allocate new root address (a1' <- Alloc())
        addresses.append(self.root_address)
        
        while (len(addresses) > 0):
            current_address = addresses.popleft()
            current_hash_key = current_node.hash_key

            if hash_key == current_hash_key: # found target
                return current_address, current_node
            elif hash_key < current_hash_key:
                next_child_index = 0 # get left child
            else:
                next_child_index = 1 # get right child

            next_address = current_node.children[next_child_index]

            # if a leaf is reached
            if next_address is None:
                break

            new_next_address = alloc(self.avl_tree_osam, self.structure) # allocate new next address (a2' <- Alloc())
            
            current_node.children[next_child_index] = new_next_address # update child address at current node (n1, aR <- a2') 
            addresses.append(new_next_address)
                
            write(current_address, current_node, self.structure) # write n1 to new current address (write(a1', n1))
            current_node = read(next_address, self.structure)
            return_available_address(next_address) # return next address to available addresses

        write(current_address, current_node, self.structure) # write back final node

    
    def height(self, key):
        height = None
        result = self._get_item(key)
        if result is not None:
            height = result[1].height
            write(result[0], result[1], self.structure) # write back final node
        return height

    def insert(self, key, value):
        self.__setitem__(key, value)

    def __setitem__(self, key, value):
        if self.hash_enabled:
            hash_key = sha3_256(str(key).encode("utf-8")).digest()
        else:
            hash_key = key

        # if tree has no nodes
        if self.root_address is None:
            self.root_address = alloc(self.avl_tree_osam, self.structure)
            write(self.root_address, SmartAVLNode(hash_key=hash_key, value=value, children=[None, None], height=1, left_height=0, right_height=0, balance=0, left_balance=None, right_balance=None), self.structure)
            return None

        current_node = read(self.root_address, self.structure) # read root (n1 <- Read(a1))
        return_available_address(self.root_address) # return root address to available addresses
    
        # find leaf node to insert new item
        nodes = []
        is_insert = True # flag for distinguishing insertions and updates
        while True:
            current_hash_key = current_node.hash_key
            nodes.append(current_node)

            if hash_key == current_hash_key: # this indicates an update to an existing node
                current_node.value = value
                is_insert = False 
                break
            elif hash_key < current_hash_key:
                next_child_index = 0 # get left child
            else:
                next_child_index = 1 # get right child

            next_address = current_node.children[next_child_index]

            # fill with dummy to know which children need to be rewritten
            # avoid assigning new children when nodes may need to be rebalanced
            current_node.children[next_child_index] = "dummy"

            if next_address is None: # leaf node reached
                nodes.append(SmartAVLNode(hash_key=hash_key, value=value, children=[None, None], height=1, left_height=0, right_height=0, balance=0, left_balance=None, right_balance=None))
                break

            current_node = read(next_address, self.structure) # read next child (n2 <- Read(n1, aR))
            return_available_address(next_address) # return address to available addresses

        # rebuild path from leaf to root
        child_address = None
        if is_insert: # rebalance tree during insertion
            
            i = len(nodes) - 1
            while (i >= 0):
                # update heights and balances as needed
                if nodes[i].hash_key != hash_key:
                    if child_hash_key < nodes[i].hash_key:
                        nodes[i].left_height = nodes[i+1].height
                        nodes[i].left_balance = nodes[i+1].balance
                    else:
                        nodes[i].right_height = nodes[i+1].height
                        nodes[i].right_balance = nodes[i+1].balance
                        
                    nodes[i].height = 1 + max(nodes[i].left_height, nodes[i].right_height)
                    nodes[i].balance = nodes[i].left_height - nodes[i].right_height

                # write back nodes that are too deep to be part of our rotation
                if i + 2 < len(nodes) - 1:
                    child_address = self._replace_dummy(child_address, nodes)

                balance = nodes[i].balance

                # left left rotation
                # before:
                #         z
                #       / 
                #     y
                #   /
                # x
                # after:
                #     y
                #   /   \
                # x       z
                if balance > 1 and hash_key < child_hash_key:
                    # print("LL rotate")
                    self._right_rotate(nodes[i+1], nodes[i])
                    child_address = self._replace_dummy(child_address, nodes) # write node_x back
                    nodes[i+1].children[0] = child_address # assign address of node_x as child to node_y
                    node_z = nodes.pop(i) # get node_z
                    child_address = alloc(self.avl_tree_osam, self.structure) 
                    nodes[i].children[1] = child_address # assign address of node_z as child to node_y
                    write(child_address, node_z, self.structure) # write node_z back
                    break

                # right right rotation
                # before:
                # x        
                #   \     
                #     y
                #       \  
                #         z
                # after:
                #     y
                #   /   \
                # x       z
                if balance < -1 and hash_key > child_hash_key:
                    # print("RR rotate")
                    self._left_rotate(nodes[i], nodes[i+1])
                    child_address = self._replace_dummy(child_address, nodes) # write node_z back
                    nodes[i+1].children[1] = child_address # assign address of node_z as child to node_y
                    node_x = nodes.pop(i) # get node_x
                    child_address = alloc(self.avl_tree_osam, self.structure)
                    nodes[i].children[0] = child_address # assign address of node_x as child to node_y
                    write(child_address, node_x, self.structure) # write node_x back
                    break

                # left right rotation
                # before:
                #         z 
                #       /  
                #     /
                #   /  
                # x       
                #  \        
                #   \       
                #    \  
                #     y  
                # # after:
                #     y
                #   /   \
                # x       z  
                if balance > 1 and hash_key > child_hash_key:
                    # print("LR rotate")
                    self._left_rotate(nodes[i+1], nodes[i+2])
                    self._right_rotate(nodes[i+2], nodes[i])

                    # put given child address at either node_z or node_x
                    node_z = nodes.pop(i)
                    node_x = nodes.pop(i)
                    for j in range(len(node_z.children)):
                        if node_z.children[j] == "dummy":
                            node_z.children[j] = child_address
                            break
                        elif node_x.children[j] == "dummy":
                            node_x.children[j] = child_address
                            break
                    
                    child_address = alloc(self.avl_tree_osam, self.structure) # get new address for node_x
                    write(child_address, node_x, self.structure) # write node_x back
                    nodes[i].children[0] = child_address # assign node_x address to node_y
                    
                    child_address = alloc(self.avl_tree_osam, self.structure) # get new address for node_z
                    write(child_address, node_z, self.structure) # write node_x back
                    break

                # right left rotation
                # before:
                # x        
                #   \     
                #     \
                #       \  
                #         z
                #       /   
                #      /   
                #     /
                #    y  
                # after:
                #     y
                #   /   \
                # x       z
                if balance < -1 and hash_key < child_hash_key:
                    # print("RL rotate")
                    self._right_rotate(nodes[i+2], nodes[i+1])
                    self._left_rotate(nodes[i], nodes[i+2])

                    # put given child address at either node_z or node_x
                    node_x = nodes.pop(i)
                    node_z = nodes.pop(i)
                    for j in range(len(node_z.children)):
                        if node_z.children[j] == "dummy":
                            node_z.children[j] = child_address
                            break
                        elif node_x.children[j] == "dummy":
                            node_x.children[j] = child_address
                            break

                    child_address = alloc(self.avl_tree_osam, self.structure) # get new address for node_x
                    write(child_address, node_x, self.structure) # write node_x back
                    nodes[i].children[0] = child_address # assign node_x address to node_y
                    
                    child_address = alloc(self.avl_tree_osam, self.structure) # get new address for node_z
                    write(child_address, node_z, self.structure) # write node_x back
                    break

                child_hash_key = nodes[i].hash_key
                i -= 1


            # update right or left balance for previous node
            if i - 1 >= 0:
                if nodes[i-1].hash_key < nodes[i].hash_key:
                    nodes[i-1].right_balance = nodes[i].balance
                else:
                    nodes[i-1].left_balance = nodes[i].balance

        # write back all (leftover) nodes
        while (nodes):
            child_address = self._replace_dummy(child_address, nodes)

        self.root_address = child_address

    def _replace_dummy(self, child_address, nodes):
        # helper function that restores addresses
        node = nodes.pop()
        for j in range(len(node.children)):
            if node.children[j] == "dummy":
                node.children[j] = child_address
        address = alloc(self.avl_tree_osam, self.structure)
        write(address, node, self.structure)
        return address
            
    def _right_rotate(self, node_y, node_z):
        # set right child of node being raised to left child of node being lowered
        node_z.children[0] = node_y.children[1]
        node_z.left_height = node_y.right_height
        node_z.height = 1 + max(node_z.left_height, node_z.right_height)
        node_z.left_balance = node_y.right_balance
        node_z.balance = node_z.left_height - node_z.right_height

        # update new parent
        node_y.right_height = node_z.height
        node_y.children[1] = "dummy"
        node_y.height = 1 + max(node_y.left_height, node_y.right_height)
        node_y.right_balance = node_z.balance
        node_y.balance = node_y.left_height - node_y.right_height

    def _left_rotate(self, node_x, node_y):
        # set left child of node being raised to right child of node being lowered
        node_x.children[1] = node_y.children[0]
        node_x.right_height = node_y.left_height
        node_x.height = 1 + max(node_x.left_height, node_x.right_height)
        node_x.right_balance = node_y.left_balance
        node_x.balance = node_x.left_height - node_x.right_height

        # update new parent
        node_y.left_height = node_x.height
        node_y.children[0] = "dummy"
        node_y.height = 1 + max(node_y.left_height, node_y.right_height)
        node_y.left_balance = node_x.balance
        node_y.balance = node_y.left_height - node_y.right_height

    def delete(self, key):
        return self.__delitem__(key)

    def __delitem__(self, key):
        # cannot delete from an empty tree
        if self.root_address is None:
            return 

        if self.hash_enabled:
            hash_key = sha3_256(str(key).encode("utf-8")).digest()
        else:
            hash_key = key

        current_node = read(self.root_address, self.structure) # read root (n1 <- Read(a1))
        return_available_address(self.root_address) # return root address to available addresses
    
        # find node to delete
        nodes = []
        result = None
        found = False
        while True:
            current_hash_key = current_node.hash_key
            nodes.append(current_node)

            if hash_key == current_hash_key: # node to delete
                result = current_node.value
                found = True
                break
            elif hash_key < current_hash_key:
                next_child_index = 0 # get left child
            else:
                next_child_index = 1 # get right child

            next_address = current_node.children[next_child_index]

            # fill with dummy to know which children need to be rewritten
            # avoid assigning new children when nodes may need to be rebalanced
            current_node.children[next_child_index] = "dummy"

            if next_address is None: # leaf node reached
                break

            current_node = read(next_address, self.structure) # read next child (n2 <- Read(n1, aR))
            return_available_address(next_address) # return root address to available addresses

        child_address = None
        if found:
            node = nodes.pop() # node to be deleted
            if node.children[0] is not None and node.children[1] is not None:
                nodes.append(node)

                # there are two children
                nodes.append(read(node.children[0], self.structure))
                return_available_address(node.children[0]) # return address to available addresses
                node.children[0] = "dummy"

                # if left child is the largest value of all values smaller than the target
                if nodes[-1].children[1] is None: 
                    child_address = nodes[-1].children[0]

                # if left child of target has right children
                else:
                    while nodes[-1].children[1] is not None:
                        nodes.append(read(nodes[-1].children[1], self.structure))
                        return_available_address(nodes[-1].children[1]) # return address to available addresses
                        nodes[-2].children[1] = "dummy"

                # update target node with max child parameters
                max_child = nodes.pop()
                node.value = max_child.value
                node.hash_key = max_child.hash_key
                child_address = max_child.children[0]

            # left child only
            elif node.children[0] is not None:
                child_address = node.children[0]

            # right child only
            elif node.children[1] is not None:
                child_address = node.children[1]
            
            i = len(nodes) - 1
            while (i >= 0):
                # update heights and balances of children
                if i == len(nodes) - 1:
                    if child_address is None:
                        child_height = 0
                        child_balance = None
                    else:
                        child_height = 1
                        child_balance = 0
                else:
                    child_height = nodes[i+1].height
                    child_balance = nodes[i+1].balance
                
                if nodes[i].children[0] == "dummy":
                    nodes[i].left_height = child_height
                    nodes[i].left_balance = child_balance
                else:
                    nodes[i].right_height = child_height
                    nodes[i].right_balance = child_balance
                        
                # update height and balance
                nodes[i].height = 1 + max(nodes[i].left_height, nodes[i].right_height)
                nodes[i].balance = nodes[i].left_height - nodes[i].right_height
                
                balance = nodes[i].balance
                left_balance = nodes[i].left_balance
                right_balance = nodes[i].right_balance

                # write back nodes that are too deep to be part of our rotation
                if i + 2 < len(nodes) - 1:
                    child_address = self._replace_dummy(child_address, nodes)

                # left left rotation
                # before:
                #         z
                #       / 
                #     y
                #   /
                # x
                # after:
                #     y
                #   /   \
                # x       z
                if balance > 1 and left_balance is not None and left_balance >= 0:
                    # print("LL rotate")

                    # if unnecessary third node has not been written back
                    if len(nodes) - 1 - i == 2:
                        child_address = self._replace_dummy(child_address, nodes)

                    # if nodes contains an i+1th node
                    left_child = None
                    if len(nodes) - 1 - i == 1:
                        # if i+1th node will be rotated (it is considered node_y in the diagram)
                        if nodes[i+1].hash_key < nodes[i].hash_key:
                            left_child = nodes.pop() # remove node_y
                            left_child.children[0] = child_address # assign node_x address as left child of node_y
                        else: 
                            # i+1th node will not be rotated and can be written back
                            child_address = self._replace_dummy(child_address, nodes)

                    # if nodes does not contain a child node to rotate
                    if left_child is None:
                        left_child = read(nodes[i].children[0], self.structure)
                        return_available_address(nodes[i].children[0]) # return address to available addresses
                        nodes[i].children[1] = child_address # assign address of i+1th node as right child of node_z

                    self._right_rotate(left_child, nodes[i])

                    # write back node_z
                    child_address = alloc(self.avl_tree_osam, self.structure) 
                    write(child_address, nodes[i], self.structure)
                    nodes.pop()
                    nodes.append(left_child)

                # right right rotation
                # before:
                # x        
                #   \     
                #     y
                #       \  
                #         z
                # after:
                #     y
                #   /   \
                # x       z
                elif balance < -1 and right_balance is not None and right_balance <= 0:
                    # print("RR rotate")
                    
                    # if unnecessary third node has not been written back
                    if len(nodes) - 1 - i == 2:
                        child_address = self._replace_dummy(child_address, nodes)

                    # if nodes contains an i+1th node
                    right_child = None
                    if len(nodes) -1 - i == 1:
                        # if i+1th node will be rotated (it is considered node_y in the diagram)
                        if nodes[i+1].hash_key > nodes[i].hash_key:
                            right_child = nodes.pop() # remove node_y
                            right_child.children[1] = child_address # assign node_z address as right child of node_y
                        else: 
                            # i+1th node will not be rotated and can be written back
                            child_address = self._replace_dummy(child_address, nodes)

                    # if nodes does not contain a child node to rotate
                    if right_child is None:
                        right_child = read(nodes[i].children[1], self.structure)
                        return_available_address(nodes[i].children[1]) # return address to available addresses
                        nodes[i].children[0] = child_address # assign address of i+1th node as left child of node_x

                    self._left_rotate(nodes[i], right_child)
                    
                    # write back node_x
                    child_address = alloc(self.avl_tree_osam, self.structure) 
                    write(child_address, nodes[i], self.structure)
                    nodes.pop()
                    nodes.append(right_child)

                # left right rotation
                # before:
                #         z 
                #       /  
                #     /
                #   /  
                # x       
                #  \        
                #   \       
                #    \  
                #     y  
                # after:
                #     y
                #   /   \
                # x       z
                elif balance > 1 and left_balance is not None and left_balance <= 0:
                    # print("LR rotate")

                    # if nodes contains i+1th and i+2th nodes
                    left_child = right_child = None
                    # nodes may contain node_x and node_y, only node_x, or neither
                    if len(nodes) - 1 - i == 2:
                        # nodes contains both
                        if nodes[i+1].hash_key < nodes[i].hash_key and nodes[i+1].hash_key < nodes[i+2].hash_key:
                            right_child = nodes.pop() # remove node_y
                            # assign child address correctly
                            if right_child.children[0] == "dummy": right_child.children[0] = child_address
                            else: right_child.children[1] = child_address
                            left_child = nodes.pop() # remove node_x
                        
                        # contains node_x
                        elif nodes[i+1].hash_key < nodes[i].hash_key:
                            # write back node that won't be rotated
                            child_address = self._replace_dummy(child_address, nodes)
                            left_child = nodes.pop() # remove node_x
                            left_child.children[0] = child_address
                        
                        # contains neither
                        else:
                            # i+1th and 1+2th nodes will not be rotated and can be written back
                            for j in range(2):
                                child_address = self._replace_dummy(child_address, nodes)
                    
                    # nodes contains only node_x or neither
                    elif len(nodes) - 1 - i == 1:
                        if nodes[i+1].hash_key < nodes[i].hash_key:
                            left_child = nodes.pop() # remove node_x
                            left_child.children[0] = child_address
                        else: 
                            child_address = self._replace_dummy(child_address, nodes)
               
                    # if left child needs to be read
                    if left_child is None:
                        left_child = read(nodes[i].children[0], self.structure)
                        return_available_address(nodes[i].children[0]) # return address to available addresses
                        nodes[i].children[0] = "dummy"
                        nodes[i].children[1] = child_address 

                    # if right child needs to be read
                    if right_child is None:
                        right_child = read(left_child.children[1], self.structure)
                        return_available_address(left_child.children[1]) # return address to available addresses
                        left_child.children[1] = "dummy"

                    self._left_rotate(left_child, right_child)
                    self._right_rotate(right_child, nodes[i])
                    
                    # write back node_z
                    child_address = alloc(self.avl_tree_osam, self.structure) 
                    write(child_address, nodes[i], self.structure)
                    right_child.children[1] = child_address
                    nodes.pop()

                    # write back node_x
                    child_address = alloc(self.avl_tree_osam, self.structure) 
                    write(child_address, left_child, self.structure)

                    nodes.append(right_child)

                # right left rotation
                # before:
                # x        
                #   \     
                #     \
                #       \  
                #         z
                #       /   
                #      /   
                #     /
                #    y 
                # after:
                #     y
                #   /   \
                # x       z
                elif balance < -1 and right_balance is not None and right_balance > 0:
                    # print("RL rotate")

                    # if nodes contains i+1th and i+2th nodes
                    left_child = right_child = None
                    # nodes may contain node_x and node_y, only node_x, or neither
                    if len(nodes) - 1 - i == 2:
                        # nodes contains both
                        if nodes[i+1].hash_key > nodes[i].hash_key and nodes[i+1].hash_key > nodes[i+2].hash_key:
                            left_child = nodes.pop() # remove node_y
                            # assign child address correctly
                            if left_child.children[0] == "dummy": left_child.children[0] = child_address
                            else: left_child.children[1] = child_address
                            right_child = nodes.pop() # remove node_x
                        
                        # contains node_x
                        elif nodes[i+1].hash_key > nodes[i].hash_key:
                            # write back node that won't be rotated
                            child_address = self._replace_dummy(child_address, nodes)
                            right_child = nodes.pop() # remove node_x
                            right_child.children[1] = child_address
                        
                        # contains neither
                        else:
                            # i+1th and 1+2th nodes will not be rotated and can be written back
                            for j in range(2):
                                child_address = self._replace_dummy(child_address, nodes)
                    
                    # nodes contains only node_x or neither
                    elif len(nodes) - 1 - i == 1:
                        if nodes[i+1].hash_key > nodes[i].hash_key:
                            right_child = nodes.pop() # remove node_x
                            right_child.children[1] = child_address
                        else: 
                            child_address = self._replace_dummy(child_address, nodes)
                    
                    # if right child needs to be read
                    if right_child is None:
                        right_child = read(nodes[i].children[1], self.structure)
                        return_available_address(nodes[i].children[1]) # return address to available addresses
                        nodes[i].children[1] = "dummy"
                        nodes[i].children[0] = child_address

                    # if right child needs to be read
                    if left_child is None:
                        left_child = read(right_child.children[0], self.structure)
                        return_available_address(right_child.children[0]) # return address to available addresses
                        right_child.children[0] = "dummy"

                    self._right_rotate(left_child, right_child)
                    self._left_rotate(nodes[i], left_child)
                    
                    # write back node_x
                    child_address = alloc(self.avl_tree_osam, self.structure) 
                    write(child_address, nodes[i], self.structure)
                    left_child.children[0] = child_address
                    nodes.pop()

                    # write back node_z
                    child_address = alloc(self.avl_tree_osam, self.structure) 
                    write(child_address, right_child, self.structure)

                    nodes.append(left_child)
            
                i -= 1

        # else:
            # print(f"No key {key} found.")

        # write back leftover nodes
        while (nodes):
            child_address = self._replace_dummy(child_address, nodes)

        self.root_address = child_address
        return result
    
    def bfs(self, key=None):
        # there are no nodes to visit if there is no root
        if self.root_address is None:
            return 
        
        if self.hash_enabled:
            hash_key = sha3_256(str(key).encode("utf-8")).digest()
        else:
            hash_key = key

        keys = []
        addresses = deque()
        nodes = deque()
        nodes.append(read(self.root_address, self.structure)) 
        return_available_address(self.root_address) # return address to available addresses
        self.root_address = alloc(self.avl_tree_osam, self.structure) 
        addresses.append(self.root_address)
        
        target_found = True if key is None else False

        while (len(addresses) > 0 and len(nodes) > 0):
            current_node = nodes.popleft()
            current_address = addresses.popleft()
            if current_node.hash_key == hash_key:
                target_found = True

                # write back children from previous nodes not along this path
                for i in range(len(addresses)):
                    write_back_node = nodes.popleft()
                    write_back_address = addresses.popleft()
                    write(write_back_address, write_back_node, self.structure)

            if target_found:
                keys.append(current_node.hash_key)

            for i in range(len(current_node.children)):
                if current_node.children[i] is not None:
                    nodes.append(read(current_node.children[i], self.structure))
                    return_available_address(current_node.children[i]) # return address to available addresses
                    current_node.children[i] = alloc(self.avl_tree_osam, self.structure)
                    addresses.append(current_node.children[i])

            write(current_address, current_node, self.structure)
        
        return keys
    
    def dfs(self, key=None):
        # there are no nodes to visit if there is no root
        if self.root_address is None:
            return 
        
        if self.hash_enabled:
            hash_key = sha3_256(str(key).encode("utf-8")).digest()
        else:
            hash_key = key

        keys = []
        addresses = deque()
        nodes = deque()
        nodes.append(read(self.root_address, self.structure)) 
        return_available_address(self.root_address) # return address to available addresses
        self.root_address = alloc(self.avl_tree_osam, self.structure) 
        addresses.append(self.root_address)
        
        target_found = True if key is None else False
        while (len(addresses) > 0 and len(nodes) > 0):
            current_node = nodes.pop()
            current_address = addresses.pop()
            if current_node.hash_key == hash_key:
                target_found = True

                # write back children from previous nodes not along this path
                if len(nodes):
                    write_back_node = nodes.popleft()
                    write_back_address = addresses.popleft()
                    write(write_back_address, write_back_node, self.structure)

            if target_found:
                keys.append(current_node.hash_key)

            for i in range(1, -1, -1):
                if current_node.children[i] is not None:
                    nodes.append(read(current_node.children[i], self.structure))
                    return_available_address(current_node.children[i]) # return address to available addresses
                    current_node.children[i] = alloc(self.avl_tree_osam, self.structure)
                    addresses.append(current_node.children[i])

            write(current_address, current_node, self.structure)
        
        return keys

    def clear(self):
        if self.root_address is None:
            return 
        
        nodes = deque()
        nodes.append(read(self.root_address, self.structure)) 
        return_available_address(self.root_address) # return address to available addresses
        self.root_address = None

        while (len(nodes) > 0):
            current_node = nodes.popleft()

            for i in range(len(current_node.children)):
                if current_node.children[i] is not None:
                    nodes.append(read(current_node.children[i], self.structure))
                    return_available_address(current_node.children[i]) # return address to available addresses
                    current_node.children[i] = None

    def reset():
        _reset()

class test_smart_avl_tree(unittest.TestCase):
    def setUp(self):
        self.l = []
        self.d = dict()
        for i in range(1, 16):
            self.l.append(i)
            self.d[i] = i
        
        # test building from lists and dicts on trees with and w/o hashing
        self.t_list = SmartAVLTree(original_elements=self.l)
        self.t_list_no_hash = SmartAVLTree(original_elements=self.l, hash_enabled=False)
        self.t_dict = SmartAVLTree(original_elements=self.d)
        self.t_dict_no_hash = SmartAVLTree(original_elements=self.d, hash_enabled=False)

    def test_search(self):
        for i in range(1, 16):
            self.assertEqual(self.t_list[i], i)
            self.assertEqual(self.t_list_no_hash[i], i)

            self.assertEqual(self.t_dict.search(i), i)
            self.assertEqual(self.t_dict_no_hash.search(i), i)

    def test_height(self):
        # can only test heights on deterministic non-hashed trees
        for i in range(1, 16, 2):
            self.assertEqual(self.t_list_no_hash.height(i), 1)
            self.assertEqual(self.t_dict_no_hash.height(i), 1)

        for i in range(2, 15, 4):
            self.assertEqual(self.t_list_no_hash.height(i), 2)
            self.assertEqual(self.t_dict_no_hash.height(i), 2)

        for i in range(4, 12, 8):
            self.assertEqual(self.t_list_no_hash.height(i), 3)
            self.assertEqual(self.t_dict_no_hash.height(i), 3)
        
        self.assertEqual(self.t_list_no_hash.height(8), 4)
        self.assertEqual(self.t_dict_no_hash.height(8), 4)

    def test_bfs(self):
        # test exact order given by bfs at each level for non-hashed
        unhashed_order1 = [8, 4, 12, 2, 6, 10, 14, 1, 3, 5, 7, 9, 11, 13, 15]

        self.assertEqual(self.t_list_no_hash.bfs(), unhashed_order1)
        self.assertEqual(self.t_dict_no_hash.bfs(), unhashed_order1)

        self.assertEqual(self.t_list_no_hash.bfs(8), unhashed_order1)
        self.assertEqual(self.t_dict_no_hash.bfs(8), unhashed_order1)

        unhashed_order2 = [12, 10, 14, 9, 11, 13, 15]

        self.assertEqual(self.t_list_no_hash.bfs(12), unhashed_order2)
        self.assertEqual(self.t_dict_no_hash.bfs(12), unhashed_order2)

        unhashed_order3 = [6, 5, 7]

        self.assertEqual(self.t_list_no_hash.bfs(6), unhashed_order3)
        self.assertEqual(self.t_dict_no_hash.bfs(6), unhashed_order3)

        unhashed_order4 = [3]

        self.assertEqual(self.t_list_no_hash.bfs(3), unhashed_order4)
        self.assertEqual(self.t_dict_no_hash.bfs(3), unhashed_order4)

        # confirm hashed values are found by bfs
        t_list_bfs = self.t_list.bfs()
        t_dict_bfs = self.t_dict.bfs()

        for i in range(1, 16):
            hash_key = sha3_256(str(i).encode("utf-8")).digest()
            self.assertIn(hash_key, t_list_bfs)
            self.assertIn(hash_key, t_dict_bfs)

    def test_dfs(self):
        # test exact order given by bfs at each level for non-hashed
        unhashed_order1 = [8, 4, 2, 1, 3, 6, 5, 7, 12, 10, 9, 11, 14, 13, 15]

        self.assertEqual(self.t_list_no_hash.dfs(), unhashed_order1)
        self.assertEqual(self.t_dict_no_hash.dfs(), unhashed_order1)

        self.assertEqual(self.t_list_no_hash.dfs(8), unhashed_order1)
        self.assertEqual(self.t_dict_no_hash.dfs(8), unhashed_order1)

        unhashed_order2 = [4, 2, 1, 3, 6, 5, 7]

        self.assertEqual(self.t_list_no_hash.dfs(4), unhashed_order2)
        self.assertEqual(self.t_dict_no_hash.dfs(4), unhashed_order2)

        unhashed_order3 = [10, 9, 11]

        self.assertEqual(self.t_list_no_hash.dfs(10), unhashed_order3)
        self.assertEqual(self.t_dict_no_hash.dfs(10), unhashed_order3)

        unhashed_order4 = [15]

        self.assertEqual(self.t_list_no_hash.bfs(15), unhashed_order4)
        self.assertEqual(self.t_dict_no_hash.bfs(15), unhashed_order4)

        # confirm hashed values are found by bfs
        t_list_dfs = self.t_list.dfs()
        t_dict_dfs = self.t_dict.dfs()

        for i in range(1, 16):
            hash_key = sha3_256(str(i).encode("utf-8")).digest()
            self.assertIn(hash_key, t_list_dfs)
            self.assertIn(hash_key, t_dict_dfs)

    def test_update(self):
        # use insert to update current items only
        for i in range(1, 16):
            self.t_list[i] = str(i)
            self.t_list_no_hash[i] = str(i)
            self.t_dict[i] = str(i)
            self.t_dict_no_hash[i] = str(i)

        for i in range(1, 16):
            self.assertEqual(self.t_list[i], str(i))
            self.assertEqual(self.t_list_no_hash[i], str(i))
            self.assertEqual(self.t_dict[i], str(i))
            self.assertEqual(self.t_dict_no_hash[i], str(i))

        for i in range(1, 16):
            self.t_list.insert(i, i)
            self.t_list_no_hash.insert(i, i)
            self.t_dict.insert(i, i)
            self.t_dict_no_hash.insert(i, i)

        for i in range(1, 16):
            self.assertEqual(self.t_list[i], i)
            self.assertEqual(self.t_list_no_hash[i], i)
            self.assertEqual(self.t_dict[i], i)
            self.assertEqual(self.t_dict_no_hash[i], i)
            
    def test_insert(self):
        # add new items
        for i in range(16, 32):
            self.t_list[i] = i
            self.t_list_no_hash[i] = i
            self.t_dict[i] = i
            self.t_dict_no_hash[i] = i

        for i in range(16, 32):
            self.assertEqual(self.t_list[i], i)
            self.assertEqual(self.t_list_no_hash[i], i)
            self.assertEqual(self.t_dict[i], i)
            self.assertEqual(self.t_dict_no_hash[i], i)

        bfs = [16, 8, 24, 4, 12, 20, 28, 2, 6, 10, 14, 18, 22, 26, 30, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31]

        self.assertEqual(self.t_list_no_hash.bfs(), bfs)
        self.assertEqual(self.t_dict_no_hash.bfs(), bfs)

        dfs = [16, 8, 4, 2, 1, 3, 6, 5, 7, 12, 10, 9, 11, 14, 13, 15, 24, 20, 18, 17, 19, 22, 21, 23, 28, 26, 25, 27, 30, 29, 31]

        self.assertEqual(self.t_list_no_hash.dfs(), dfs)
        self.assertEqual(self.t_dict_no_hash.dfs(), dfs)

    def test_delete(self):
        # add new items
        for i in range(16, 32):
            self.t_list.delete(i)
            self.t_list_no_hash.delete(i)
            del self.t_dict[i]
            del self.t_dict_no_hash[i] 

        for i in range(16, 32):
            self.assertEqual(self.t_list[i], None)
            self.assertEqual(self.t_list_no_hash[i], None)
            self.assertEqual(self.t_dict[i], None)
            self.assertEqual(self.t_dict_no_hash[i], None)

        bfs = [8, 4, 12, 2, 6, 10, 14, 1, 3, 5, 7, 9, 11, 13, 15]

        self.assertEqual(self.t_list_no_hash.bfs(), bfs)
        self.assertEqual(self.t_dict_no_hash.bfs(), bfs)

        dfs = [8, 4, 2, 1, 3, 6, 5, 7, 12, 10, 9, 11, 14, 13, 15]

        self.assertEqual(self.t_list_no_hash.dfs(), dfs)
        self.assertEqual(self.t_dict_no_hash.dfs(), dfs)

    def test_clear(self):
        self.t_list.clear()
        self.t_list_no_hash.clear()
        self.t_dict.clear()
        self.t_dict_no_hash.clear()

        for i in range(1, 16):
            self.assertEqual(self.t_list[i], None)
            self.assertEqual(self.t_list_no_hash[i], None)
            self.assertEqual(self.t_dict[i], None)
            self.assertEqual(self.t_dict_no_hash[i], None)

    def test_rotations(self):
        # this specific order tests all 4 possible rotations during insertions and deletions
        order = [25, 45, 16, 24, 21, 30, 41, 9, 19, 33, 43, 17, 46, 34, 4, 11, 42, 32, 14, 27, 47, 48, 22, 2, 3, 49, 20, 18, 39, 28, 6, 36, 12, 15, 38, 1, 8, 37, 26, 13, 23, 5, 7, 35, 10, 44, 31, 0, 29, 40]

        for i in order:
            self.t_list_no_hash[i] = i
        
        for i in order:
            self.assertEqual(self.t_list_no_hash[i], i)

        for i in range(len(order)):
            del self.t_list_no_hash[order[i]]
            bfs = self.t_list_no_hash.bfs()
            for j in order[i+1:]:
                self.assertIn(j, bfs)

if __name__ == "__main__":   
    labels = [i for i in range(50)]   
    tree = SmartAVLTree(original_elements=labels, hash_enabled=True)

    order = [44, 40, 23, 37, 1, 17, 2, 30, 19, 25, 43, 45, 33, 5, 16, 49, 41, 31, 27, 13, 15, 22, 42, 34, 28, 20, 0, 36, 26, 29, 24, 32, 47, 8, 4, 18, 35, 14, 21, 11, 38, 39, 12, 3, 10, 7, 6, 46, 9, 48]

    print(tree[44])
    print(tree[444])

    unittest.main(exit=False)