import unittest
from cascading_params import CascadingParams

class TestCascadingParams(unittest.TestCase):
    def setUp(self):
        self.cp = CascadingParams()

    def test_add_to_start_single_element(self):
        self.cp.add_to_start('test')
        self.assertEqual(self.cp.param_list[0], 'test')

    def test_add_to_start_multiple_elements(self):
        self.cp.add_to_start('test1')
        self.cp.add_to_start('test2')
        self.assertEqual(self.cp.param_list[0], 'test2')
        self.assertEqual(self.cp.param_list[1], 'test1')

    def test_add_to_start_none(self):
        self.cp.add_to_start(None)
        self.assertEqual(self.cp.param_list[0], None)

if __name__ == '__main__':
    unittest.main()