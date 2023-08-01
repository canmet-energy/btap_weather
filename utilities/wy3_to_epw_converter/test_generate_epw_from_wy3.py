import filecmp
from generate_epw_from_wy3 import *

main_program()
expected_file_name = "expected_output.epw"
test_file_name = "output.epw"
assert(filecmp.cmp(test_file_name,expected_file_name))

