import sys
import os

class TestSetup:
    """ This class is used to setup the test environment. It adds the src directory to the sys.path. """
    def __init__(self):
        """ Constructor of the TestSetup class. """
        self.current_dir = os.getcwd()
        self.src_dir = os.path.join(self.current_dir, "src")
        sys.path.append(self.src_dir)
        print(sys.path)
# # Get the absolute path of the current script's directory
# current_dir = os.getcwd()# os.path.dirname(os.path.abspath(__file__))

# # Construct the path to the src directory
# src_dir = os.path.join(current_dir, "src")

# # Add the src directory to the sys.path
# sys.path.append(src_dir)

# print(sys.path)