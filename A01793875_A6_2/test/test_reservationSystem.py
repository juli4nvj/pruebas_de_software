import unittest
from A01793875_A6_2.reservation_system import Manager
from A01793875_A6_2.hotel import Hotel
from A01793875_A6_2.customers import Customer
from A01793875_A6_2.reservation import Reservation
import json


class TestManager(unittest.TestCase):
    def setUp(self):
        # Create a Manager instance for testing
        self.manager = Manager()
        # Create sample data for testing
        self.sample_hotels = [
            Hotel("Hotel A", "Location A", 10, 10),
            Hotel("Hotel B", "Location B", 20, 20)
        ]
        self.sample_customers = [
            Customer("John Doe", "john@example.com", "123456789"),
            Customer("Jane Doe", "jane@example.com", "987654321")
        ]
        self.sample_reservations = [
            Reservation("John Doe", "Hotel A", 1),
            Reservation("Jane Doe", "Hotel B", 2)
        ]

    def test_create_hotel(self):
        self.manager.create_hotel("Hotel C", "Location C", 30)
        self.assertEqual(len(self.manager.hotels), 1)

    def test_delete_hotel(self):
        self.manager.hotels = self.sample_hotels
        self.manager.delete_hotel("Hotel A")
        self.assertEqual(len(self.manager.hotels), 1)

    def test_reserve_room(self):
        self.manager.hotels = self.sample_hotels
        self.manager.customers = self.sample_customers
        self.manager.reserve_room("John Doe", "Hotel A", 1)
        self.assertEqual(len(self.manager.reservations), 1)

    def test_create_customer(self):
        self.manager.create_customer("New Customer", "new@example.com", "999999999")
        self.assertEqual(len(self.manager.customers), 1)

    def test_delete_customer(self):
        self.manager.customers = self.sample_customers
        self.manager.delete_customer("John Doe")
        self.assertEqual(len(self.manager.customers), 1)

    def test_load_data(self):
        # Create sample data files
        with open('D:\\TEC_CODE\\A01793875_A6_2\\data\\hotels.json', 'w') as f:
            json.dump([hotel.to_dict() for hotel in self.sample_hotels], f)
        with open('D:\\TEC_CODE\\A01793875_A6_2\\data\\customers.json', 'w') as f:
            json.dump([customer.to_dict() for customer in self.sample_customers], f)
        with open('D:\\TEC_CODE\\A01793875_A6_2\\data\\reservations.json', 'w') as f:
            json.dump([reservation.to_dict() for reservation in self.sample_reservations], f)
        # Call load_data method
        self.manager.load_data()

    def test_delete_customer(self):
        self.manager.customers = self.sample_customers
        self.manager.delete_customer("John Doe")
        self.assertEqual(len(self.manager.customers), 1)

    def test_create_customer(self):
        self.manager.create_customer("New Customer", "new@example.com", "999999999")
        self.assertEqual(len(self.manager.customers), 1)

    def test_modify_hotel_info(self):
        self.manager.create_hotel("Hotel ABC", "Location XYZ", 10)
        self.manager.modify_hotel_info("Hotel ABC", "Hotel XYZ", "Location ABC", 20)
        hotel = self.manager.hotels[0]
        self.assertEqual(hotel.name, "Hotel XYZ")
        self.assertEqual(hotel.location, "Location ABC")
        self.assertEqual(hotel.capacity, 20)



if __name__ == "__main__":
    unittest.main()
