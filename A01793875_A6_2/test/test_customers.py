import unittest
from A01793875_A6_2.customers import Customer


class TestCustomer(unittest.TestCase):
    def test_init(self):
        customer = Customer("John Doe", "john@example.com", "123456789")
        self.assertEqual(customer.name, "John Doe")
        self.assertEqual(customer.email, "john@example.com")
        self.assertEqual(customer.phone, "123456789")

    def test_update_info(self):
        customer = Customer("Jane Doe", "jane@example.com", "987654321")
        customer.update_info("Jane Smith", "jane.smith@example.com", "555555555")
        self.assertEqual(customer.name, "Jane Smith")
        self.assertEqual(customer.email, "jane.smith@example.com")
        self.assertEqual(customer.phone, "555555555")

    def test_to_dict(self):
        customer = Customer("Alice", "alice@example.com", "999888777")
        expected_dict = {
            'name': "Alice",
            'email': "alice@example.com",
            'phone': "999888777"
        }
        self.assertEqual(customer.to_dict(), expected_dict)

if __name__ == '__main__':
    unittest.main()
